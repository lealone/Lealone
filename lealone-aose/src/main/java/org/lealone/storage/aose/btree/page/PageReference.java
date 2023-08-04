/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.session.Session;
import org.lealone.storage.aose.btree.BTreeStorage;
import org.lealone.storage.page.PageOperationHandler;

public class PageReference {

    private static final AtomicReferenceFieldUpdater<PageReference, PageOperationHandler> //
    lockUpdater = AtomicReferenceFieldUpdater.newUpdater(PageReference.class, PageOperationHandler.class,
            "lockOwner");
    protected volatile PageOperationHandler lockOwner;

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
    }

    public PageReference getParentRef() {
        return parentRef;
    }

    // 记录访问过page的事务id，如果这些事务都结束了那就可以回收page了
    private final ConcurrentSkipListSet<Long> tids = new ConcurrentSkipListSet<>();

    public ConcurrentSkipListSet<Long> getTids() {
        return tids;
    }

    public boolean tryLock(PageOperationHandler newLockOwner) {
        // 前面的操作被锁住了就算lockOwner相同后续的也不能再继续
        if (newLockOwner == lockOwner)
            return false;
        while (true) {
            if (lockUpdater.compareAndSet(this, null, newLockOwner))
                return true;
            PageOperationHandler owner = lockOwner;
            if (owner != null) {
                owner.addWaitingHandler(newLockOwner);
            }
            // 解锁了，或者又被其他线程锁住了
            if (lockOwner == null || lockOwner != owner)
                continue;
            else
                return false;
        }
    }

    public void unlock() {
        if (lockOwner != null) {
            PageOperationHandler owner = lockOwner;
            lockOwner = null;
            owner.wakeUpWaitingHandlers();
        }
    }

    public boolean isLocked() {
        return lockOwner != null;
    }

    public boolean isRoot() {
        return false;
    }

    private static final AtomicReferenceFieldUpdater<PageReference, PageInfo> //
    pageInfoUpdater = AtomicReferenceFieldUpdater.newUpdater(PageReference.class, PageInfo.class,
            "pInfo");

    private final BTreeStorage bs;
    private final boolean inMemory;
    private volatile PageInfo pInfo; // 已经确保不会为null

    public PageReference(BTreeStorage bs) {
        this.bs = bs;
        inMemory = bs.isInMemory();
        pInfo = new PageInfo();
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

    public Page getPage() {
        return pInfo.page;
    }

    private long getTid() {
        Object t = Thread.currentThread();
        if (t instanceof PageOperationHandler) {
            Session s = ((PageOperationHandler) t).getSession();
            if (s != null)
                return s.getCurrentTid();
        }
        return 0;
    }

    // 多线程读page也是线程安全的
    public Page getOrReadPage() {
        boolean ok = lockOwner == null && !inMemory;
        if (ok) {
            long tid = getTid();
            if (tid > 0) {
                if (tids.add(tid))
                    bs.getBTreeGC().addUsedMemory(32);
            }
        }
        PageInfo pInfo = this.pInfo;
        Page p = pInfo.page; // 先取出来，GC线程可能把pInfo.page置null
        if (p != null) {
            if (ok) // 避免反复调用
                pInfo.updateTime();
            return p;
        } else {
            ByteBuffer buff = pInfo.buff; // 先取出来，GC线程可能把pInfo.buff置null
            if (buff != null) {
                p = bs.readPage(pInfo, this, pInfo.pos, buff, pInfo.pageLength);
                bs.getBTreeGC().addUsedMemory(p.getMemory());
            } else {
                try {
                    p = bs.readPage(pInfo, this);
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
            // p有可能为null，那就再读一次
            return p != null ? p : getOrReadPage();
        }
    }

    public boolean replacePage(PageInfo expect, PageInfo update) {
        return pageInfoUpdater.compareAndSet(this, expect, update);
    }

    public void replacePage(Page page) {
        Page oldPage = this.pInfo.page;
        if (oldPage == page)
            return;
        if (Page.ASSERT) {
            if (!isRoot() && !isLocked())
                DbException.throwInternalError("not locked");
        }
        PageInfo pInfo;
        if (page != null) {
            pInfo = new PageInfo();
            pInfo.pos = page.getPos();
            pInfo.page = page;
        } else {
            pInfo = new PageInfo();
        }
        this.pInfo = pInfo;
        if (oldPage != null)
            oldPage.markDirtyBottomUp();
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
}
