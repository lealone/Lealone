/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.lock.Lock;
import com.lealone.db.lock.Lockable;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.scheduler.SchedulerLock;
import com.lealone.storage.aose.btree.BTreeStorage;
import com.lealone.storage.page.IPageReference;
import com.lealone.storage.page.PageListener;

//内存占用32+16=48字节
public class PageReference implements IPageReference {

    private static final AtomicReferenceFieldUpdater<PageReference, PageInfo> //
    pageInfoUpdater = AtomicReferenceFieldUpdater.newUpdater(PageReference.class, PageInfo.class,
            "pInfo");

    private volatile PageInfo pInfo; // 已经确保不会为null
    private PageReference parentRef;

    private final BTreeStorage bs;
    private final SchedulerLock schedulerLock = new SchedulerLock();

    public PageReference(BTreeStorage bs) {
        this.bs = bs;
        pInfo = new PageInfo();
        pInfo.setPageLock(createPageLock());
        pInfo.metaVersion = bs.getMap().getValueType().getMetaVersion();
    }

    public PageReference(BTreeStorage bs, long pos) {
        this(bs);
        pInfo.pos = pos;
    }

    public PageReference(BTreeStorage bs, Page page) {
        this(bs);
        pInfo.page = page;
    }

    private PageLock createPageLock() {
        PageLock pageLock = new PageLock();
        pageLock.setPageListener(new PageListener(this));
        return pageLock;
    }

    public void setNewPageLock() {
        getPageInfo().setPageLock(createPageLock());
    }

    public boolean isRoot() {
        return false;
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
    public PageListener getPageListener() {
        return pInfo.getPageListener();
    }

    @Override
    public PageLock getLock() {
        return pInfo.getPageLock();
    }

    @Override
    public void remove(Object key) {
        bs.getMap().remove(this, key);
    }

    @Override
    public void addPageUsedMemory(int delta) {
        Page p = pInfo.getPage();
        if (p != null)
            p.addMemory(delta);
    }

    @Override
    public int getMetaVersion() {
        return pInfo.metaVersion;
    }

    @Override
    public void setMetaVersion(int mv) {
        pInfo.metaVersion = mv;
    }

    @Override
    public Object[] getValues() {
        return getOrReadPage().getValues();
    }

    public boolean isDataStructureChanged() {
        return pInfo.isDataStructureChanged();
    }

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

    public Page getOrReadPage() {
        PageInfo pInfo = this.pInfo;
        if (pInfo.isDataStructureChanged()) { // 发生 page split 或 page remove
            return pInfo.getNewRef().getOrReadPage();
        }
        if (bs.getMap().isInMemory())
            return pInfo.page;
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
            addUsedMemory(memory);
            return p;
        } else {
            return getOrReadPage();
        }
    }

    public boolean replacePage(PageInfo expect, PageInfo update) {
        return pageInfoUpdater.compareAndSet(this, expect, update);
    }

    // 这个方法是在初始化root page时执行
    // 或对page已经加锁且标记为脏页后，写操作完成了才执行
    public void replacePage(Page newPage) {
        if (Page.ASSERT) {
            if (!isRoot() && !isLocked() && !pInfo.isDirty())
                DbException.throwInternalError("not locked");
        }
        // 不能这样直接替换，否则其他线程标记脏页时有可能使用旧的page
        // pInfo.page = newPage;
        while (true) {
            PageInfo pInfoOld = getPageInfo();
            PageInfo pInfoNew = pInfoOld.copy(false);
            pInfoNew.page = newPage;
            if (replacePage(pInfoOld, pInfoNew))
                break;
        }
    }

    // 不改变page，只是改变pos
    public void markDirtyPage() {
        markDirtyPage(getPageListener());
    }

    @Override
    public Lockable markDirtyPage(Object key, PageListener oldPageListener) {
        int ret = markDirtyPage0(oldPageListener);
        if (ret > 0 && key != null) {
            Page page = bs.getMap().gotoLeafPage(key);
            int index = page.binarySearch(key);
            if (index >= 0) {
                Object value = page.getValue(index);
                if (value instanceof Lockable) {
                    // 如果page被切割过了，调用markDirtyPage前已经加了行锁，这时替换锁后重试是安全的
                    // 如果是被垃圾收集过了，这时不能替换锁，直接返回新的记录重试即可
                    Lockable lockable = (Lockable) value;
                    if (ret == 2) {
                        Lock old = lockable.getLock();
                        lockable.setLock(page.getRef().getLock());
                        if (old != null)
                            old.unlockFast();
                    } else {
                        lockable.getLock().setPageListener(page.getRef().getPageListener());
                    }
                    return lockable;
                }
            }
        }
        return null;
    }

    public boolean markDirtyPage(PageListener oldPageListener) {
        return markDirtyPage0(oldPageListener) == 0;
    }

    private int markDirtyPage0(PageListener oldPageListener) {
        int ret = markDirtyPage1(oldPageListener);
        // 从下往上标记脏页,只要有一个新的PageListener跟旧的不一样，那就退出，然后调用者会重新从root获取新的
        if (ret == 0) {
            PageReference parentRef = getParentRef();
            while (parentRef != null) {
                oldPageListener = oldPageListener.getParent();
                ret = parentRef.markDirtyPage1(oldPageListener);
                if (ret == 0) {
                    parentRef = parentRef.getParentRef();
                } else {
                    return ret;
                }
            }
        }
        return ret;
    }

    // 0：成功
    // 1：被垃圾收集过了
    // 2：page被切割过了
    private int markDirtyPage1(PageListener oldPageListener) {
        while (true) {
            PageInfo pInfoOld = this.pInfo;
            if (pInfoOld.getPageListener() != oldPageListener || pInfoOld.page == null) {
                return 1;
            }
            if (pInfoOld.isSplitted()) {
                return 2;
            }
            PageInfo pInfoNew = pInfoOld.copy(0);
            pInfoNew.buff = null; // 废弃了
            if (replacePage(pInfoOld, pInfoNew)) {
                if (Page.ASSERT) {
                    checkPageInfo(pInfoNew);
                }
                if (pInfoOld.getPos() != 0) {
                    addRemovedPage(pInfoOld.getPos());
                    addUsedMemory(-pInfoOld.getBuffMemory());
                }
                if (pInfoNew.page instanceof ColumnStorageLeafPage) {
                    ((ColumnStorageLeafPage) pInfoNew.page).markAllColumnPagesDirty();
                }
                return 0;
            } else if (getPageInfo().getPos() != 0) { // 刷脏页线程刚写完，需要重试
                continue;
            } else {
                if (this.pInfo.getPageListener() != oldPageListener)
                    return 1;
                return 0; // 如果pos为0就不需要试了
            }
        }
    }

    private void checkPageInfo(PageInfo pInfoNew) {
        if (pInfoNew.page == null && pInfoNew.pos == 0) {
            DbException.throwInternalError();
        }
    }

    private void addUsedMemory(long delta) {
        if (delta != 0) {
            bs.getBTreeGC().addUsedMemory(delta);
        }
    }

    private void addRemovedPage(long pos) {
        bs.getChunkManager().addRemovedPage(pos);
    }

    // 刷完脏页后需要用新的位置更新
    public void updatePage(long newPos, PageInfo pInfoOld) {
        updatePage(newPos, pInfoOld, false);
    }

    // 重写page后会把clearPage设为true来调用
    public void updatePage(long newPos, PageInfo pInfoOld, boolean clearPage) {
        PageInfo pInfoNew = pInfoOld.copy(newPos);
        pInfoNew.buff = null; // 废弃了
        if (clearPage)
            pInfoNew.page = null;
        if (replacePage(pInfoOld, pInfoNew)) {
            if (Page.ASSERT) {
                checkPageInfo(pInfoNew);
            }
            addUsedMemory(-pInfoOld.getBuffMemory());
        } else {
            // 当前page又被标记为脏页了，此时把写完的page标记为删除
            addRemovedPage(newPos);
        }
    }

    public boolean canGc() {
        PageInfo pInfo = this.pInfo;
        if (pInfo.page == null && pInfo.buff == null)
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
            pInfoNew.setPageLock(createPageLock());
            if (replacePage(pInfoOld, pInfoNew)) {
                if (Page.ASSERT) {
                    checkPageInfo(pInfoNew);
                }
                addUsedMemory(-memory);
                if (gcType == 1)
                    return pInfoNew;
                else
                    return pInfoOld;
            }
        }
        return null;
    }
}
