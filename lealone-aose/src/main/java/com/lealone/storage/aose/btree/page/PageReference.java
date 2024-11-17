/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.scheduler.SchedulerLock;
import com.lealone.storage.aose.btree.BTreeStorage;
import com.lealone.storage.aose.btree.page.PageInfo.SplittedPageInfo;
import com.lealone.storage.aose.btree.page.PageOperations.TmpNodePage;
import com.lealone.storage.page.IPageReference;
import com.lealone.storage.page.PageListener;

//内存占用32+16=48字节
public class PageReference implements IPageReference {

    private final SchedulerLock schedulerLock = new SchedulerLock();

    @Override
    public PageListener getPageListener() {
        return pInfo.getPageListener();
    }

    public void setPageListener(PageListener pageListener) {
        pInfo.setPageListener(pageListener);
        if (parentRef != null) {
            pageListener.setParent(parentRef.getPageListener());
        }
    }

    private boolean dataStructureChanged; // 比如发生了切割或page从父节点中删除

    public boolean isDataStructureChanged() {
        return dataStructureChanged;
    }

    public void setDataStructureChanged(boolean dataStructureChanged) {
        this.dataStructureChanged = dataStructureChanged;
    }

    private PageReference parentRef;

    public void setParentRef(PageReference parentRef) {
        this.parentRef = parentRef;
        if (parentRef != null) {
            getPageListener().setParent(parentRef.getPageListener());
        }
    }

    public PageReference getParentRef() {
        return parentRef;
    }

    public boolean tryLock(InternalScheduler newLockOwner, boolean waitingIfLocked) {
        return schedulerLock.tryLock(newLockOwner, waitingIfLocked);
    }

    public void unlock() {
        schedulerLock.unlock();
    }

    public boolean isLocked() {
        return schedulerLock.isLocked();
    }

    public boolean isRoot() {
        return false;
    }

    private static final AtomicReferenceFieldUpdater<PageReference, PageInfo> //
    pageInfoUpdater = AtomicReferenceFieldUpdater.newUpdater(PageReference.class, PageInfo.class,
            "pInfo");

    private final BTreeStorage bs;
    private volatile PageInfo pInfo; // 已经确保不会为null

    public PageReference(BTreeStorage bs) {
        this.bs = bs;
        pInfo = new PageInfo();
        setPageListener(new PageListener(this));
    }

    public PageReference(BTreeStorage bs, long pos) {
        this(bs);
        pInfo.pos = pos;
    }

    public PageReference(BTreeStorage bs, Page page) {
        this(bs);
        pInfo.page = page;
    }

    public PageInfo getPageInfo() {
        return pInfo;
    }

    public long getPos() {
        return pInfo.pos;
    }

    public Page getPage() {
        return pInfo.page;
    }

    public boolean isLeafPage() {
        Page p = pInfo.page;
        if (p != null)
            return p.isLeaf();
        else
            return PageUtils.isLeafPage(pInfo.pos);
    }

    public boolean isNodePage() {
        Page p = pInfo.page;
        if (p != null)
            return p.isNode();
        else
            return PageUtils.isNodePage(pInfo.pos);
    }

    @Override
    public String toString() {
        return "PageReference[" + pInfo.pos + "]";
    }

    @Override
    public Page getOrReadPage() {
        PageInfo pInfo = this.pInfo;
        if (pInfo.isSplitted()) { // 发生 split 了
            return pInfo.getNewRef().getOrReadPage();
        }
        Page p = pInfo.page; // 先取出来，GC线程可能把pInfo.page置null
        if (p != null) {
            pInfo.updateTime();
            return p;
        } else {
            return readPage(pInfo);
        }
    }

    // 多线程读page也是线程安全的
    private Page readPage(PageInfo pInfoOld) {
        Page p;
        PageInfo pInfoNew;
        ByteBuffer buff = pInfoOld.buff; // 先取出来，GC线程可能把pInfo.buff置null
        if (buff != null) {
            pInfoNew = bs.readPage(this, pInfoOld.pos, buff, pInfoOld.pageLength);
        } else {
            try {
                pInfoNew = bs.readPage(this, pInfoOld.pos);
            } catch (RuntimeException e) {
                // 执行Compact时如果被重写的chunk文件已经删除了，此时正好用老的pos读page会导致异常
                // 直接用Compact线程读好的page即可
                p = this.pInfo.page;
                if (p != null)
                    return p;
                else
                    throw e;
            }
        }
        pInfoNew.updateTime();
        if (replacePage(pInfoOld, pInfoNew)) {
            p = pInfoNew.page;
            int memory = p.getMemory();
            if (buff == null)
                memory += pInfoNew.getBuffMemory();
            bs.getBTreeGC().addUsedMemory(memory);
            return p;
        } else {
            return getOrReadPage();
        }
    }

    private boolean replacePage(PageInfo expect, PageInfo update) {
        return pageInfoUpdater.compareAndSet(this, expect, update);
    }

    // 强制改变page和pos
    // 在这里不需要把父节点标记为脏页，事务提交前会调用markDirtyBottomUp把涉及的所有节点标记为脏页
    public void replacePage(Page newPage) {
        while (true) {
            PageInfo pInfoOld = this.pInfo;
            Page oldPage = pInfoOld.page;
            if (oldPage == newPage)
                return;
            if (Page.ASSERT) {
                if (!isRoot() && !isLocked())
                    DbException.throwInternalError("not locked");
            }
            if (oldPage != null) {
                PageInfo pInfoNew = pInfoOld.copy(0);
                pInfoNew.page = newPage;
                pInfoNew.buff = null;
                if (replacePage(pInfoOld, pInfoNew)) {
                    if (pInfoOld.getPos() != 0) {
                        addRemovedPage(pInfoOld.getPos());
                        bs.getBTreeGC().addUsedMemory(-pInfoOld.getBuffMemory());
                    }
                    return;
                } else {
                    continue;
                }
            } else {
                this.pInfo.page = newPage;
                this.pInfo.updateTime();
                return;
            }
        }
    }

    private void checkPageInfo(PageInfo pInfoNew) {
        if (pInfoNew.page == null && pInfoNew.pos == 0) {
            DbException.throwInternalError();
        }
    }

    @Override
    public boolean markDirtyPage(PageListener oldPageListener) {
        if (markDirtyPage0(oldPageListener)) {
            PageReference parentRef = getParentRef();
            while (parentRef != null) {
                oldPageListener = oldPageListener.getParent();
                if (markDirtyPage0(oldPageListener)) {
                    parentRef = parentRef.getParentRef();
                } else {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void markDirtyBottomUp() {
        Page p = pInfo.page;
        if (p != null)
            p.markDirtyBottomUp();
    }

    private boolean markDirtyPage0(PageListener oldPageListener) {
        while (true) {
            PageInfo pInfoOld = this.pInfo;
            if (pInfoOld.getPageListener() != oldPageListener)
                return false;
            if (pInfoOld.isSplitted() || pInfoOld.page == null) {
                return true;
            }
            PageInfo pInfoNew = pInfoOld.copy(0);
            pInfoNew.buff = null; // 废弃了
            pInfoNew.markDirtyCount++;
            if (replacePage(pInfoOld, pInfoNew)) {
                if (Page.ASSERT) {
                    checkPageInfo(pInfoNew);
                }
                if (pInfoOld.getPos() != 0) {
                    addRemovedPage(pInfoOld.getPos());
                    bs.getBTreeGC().addUsedMemory(-pInfoOld.getBuffMemory());
                }
                return true;
            } else if (getPageInfo().getPos() != 0) { // 刷脏页线程刚写完，需要重试
                continue;
            } else {
                if (this.pInfo.getPageListener() != oldPageListener)
                    return false;
                return true; // 如果pos为0就不需要试了
            }
        }
    }

    // 不改变page，只是改变pos
    public void markDirtyPage() {
        while (true) {
            PageInfo pInfoOld = this.pInfo;
            if (pInfoOld.isSplitted() || pInfoOld.page == null) {
                return;
            }
            PageInfo pInfoNew = pInfoOld.copy(0);
            pInfoNew.buff = null; // 废弃了
            pInfoNew.markDirtyCount++;
            if (replacePage(pInfoOld, pInfoNew)) {
                if (Page.ASSERT) {
                    checkPageInfo(pInfoNew);
                }
                if (pInfoOld.getPos() != 0) {
                    addRemovedPage(pInfoOld.getPos());
                    bs.getBTreeGC().addUsedMemory(-pInfoOld.getBuffMemory());
                }
                return;
            } else if (getPageInfo().getPos() != 0) { // 刷脏页线程刚写完，需要重试
                continue;
            } else {
                return; // 如果pos为0就不需要试了
            }
        }
    }

    private void addRemovedPage(long pos) {
        bs.getChunkManager().addRemovedPage(pos);
    }

    private boolean isDirtyPage(Page oldPage, long oldMarkDirtyCount) {
        // 如果page被split了，刷脏页时要标记为删除
        if (isDataStructureChanged())
            return true;
        PageInfo pInfo = this.pInfo;
        if (pInfo.page != oldPage)
            return true;
        if (pInfo.pos == 0 && pInfo.markDirtyCount != oldMarkDirtyCount)
            return true;
        return false;
    }

    // 刷完脏页后需要用新的位置更新，如果当前page不是oldPage了，那么把oldPage标记为删除
    public void updatePage(long newPos, Page oldPage, PageInfo pInfoSaved, boolean isLocked) {
        PageInfo pInfoOld = pInfoSaved;
        // 两种情况需要删除当前page：1.当前page已经发生新的变动; 2.已经被标记为脏页
        if (isLocked || isDirtyPage(oldPage, pInfoSaved.markDirtyCount)) {
            addRemovedPage(newPos);
            return;
        }
        PageInfo pInfoNew = pInfoOld.copy(newPos);
        pInfoNew.buff = null; // 废弃了
        if (replacePage(pInfoOld, pInfoNew)) {
            if (Page.ASSERT) {
                checkPageInfo(pInfoNew);
            }
            bs.getBTreeGC().addUsedMemory(-pInfoOld.getBuffMemory());
        } else {
            if (isDirtyPage(oldPage, pInfoSaved.markDirtyCount)) {
                addRemovedPage(newPos);
            } else {
                if (Page.ASSERT) {
                    checkPageInfo(pInfoNew);
                }
            }
        }
    }

    public boolean canGc() {
        PageInfo pInfo = this.pInfo;
        Page p = pInfo.page;
        if (p == null && pInfo.buff == null)
            return false;
        if (pInfo.pos == 0) // pos为0时说明page被修改了，不能回收
            return false;
        if (isLocked()) // 如果page处于被加锁的状态，不能回收
            return false;
        return true;
    }

    // gcType: 0释放page和buff、1释放page、2释放buff
    public PageInfo gcPage(PageInfo pInfoOld, int gcType) {
        if (getPageInfo() != pInfoOld) // 发生变动了，不需要进一步的操作
            return null;
        long memory = 0;
        boolean gc = false;
        Page p = pInfoOld.page;
        ByteBuffer buff = pInfoOld.buff;
        PageInfo pInfoNew = null;
        if (gcType == 0 && (p != null || buff != null)) {
            memory = pInfoOld.getTotalMemory();
            pInfoNew = pInfoOld.copy(true);
            pInfoNew.releasePage();
            pInfoNew.releaseBuff();
            gc = true;
        } else if (gcType == 1 && p != null) {
            memory = pInfoOld.getPageMemory();
            pInfoNew = pInfoOld.copy(true);
            pInfoNew.releasePage();
            gc = true;
        } else if (gcType == 2 && buff != null) {
            memory = pInfoOld.getBuffMemory();
            pInfoNew = pInfoOld.copy(true);
            pInfoNew.releaseBuff();
            gc = true;
        }
        if (gc) {
            pInfoNew.setPageListener(new PageListener(this));
            if (replacePage(pInfoOld, pInfoNew)) {
                if (Page.ASSERT) {
                    checkPageInfo(pInfoNew);
                }
                bs.getBTreeGC().addUsedMemory(-memory);
                if (gcType == 1)
                    return pInfoNew;
                else
                    return pInfoOld;
            }
        }
        return null;
    }

    public static void replaceSplittedPage(TmpNodePage tmpNodePage, PageReference parentRef,
            PageReference ref, Page newPage) {
        while (true) {
            PageInfo pInfoOld1 = parentRef.getPageInfo();
            PageInfo pInfoOld2 = ref.getPageInfo();
            PageInfo pInfoNew = new PageInfo();
            pInfoNew.page = newPage;
            pInfoNew.updateTime(pInfoOld1);
            if (!parentRef.replacePage(pInfoOld1, pInfoNew))
                continue;
            if (ref != parentRef) {
                // 如果其他事务引用的是一个已经split的节点，让它重定向到临时的中间节点
                PageReference tmpRef = tmpNodePage.parent.getRef();
                tmpRef.setParentRef(parentRef);
                pInfoNew = new SplittedPageInfo(tmpRef);
                pInfoNew.page = tmpNodePage.parent;
                if (!ref.replacePage(pInfoOld2, pInfoNew))
                    continue;
            }
            break;
        }
    }
}
