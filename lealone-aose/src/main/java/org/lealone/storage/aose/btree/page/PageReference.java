/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

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

    public boolean isRoot() {
        return false;
    }

    private static final AtomicReferenceFieldUpdater<PageReference, PageInfo> //
    pageInfoUpdater = AtomicReferenceFieldUpdater.newUpdater(PageReference.class, PageInfo.class,
            "pInfo");

    private volatile PageInfo pInfo; // 已近确保不会为null

    public PageReference() {
        pInfo = new PageInfo();
    }

    public PageReference(long pos) {
        this();
        pInfo.pos = pos;
    }

    public PageReference(Page page) {
        this();
        pInfo.page = page;
    }

    public long getPos() {
        return pInfo.pos;
    }

    public Page getPage() {
        return pInfo.page;
    }

    public PageInfo getPageInfo() {
        return pInfo;
    }

    public boolean replacePage(PageInfo expect, PageInfo update) {
        return pageInfoUpdater.compareAndSet(this, expect, update);
    }

    public void replacePage(Page page) {
        if (this.pInfo.page == page)
            return;
        PageInfo pInfo;
        if (page != null) {
            pInfo = new PageInfo();
            pInfo.pos = page.getPos();
            pInfo.page = page;
        } else {
            pInfo = new PageInfo();
        }
        this.pInfo = pInfo;
    }

    public boolean isLeafPage() {
        if (pInfo.page != null)
            return pInfo.page.isLeaf();
        else
            return PageUtils.isLeafPage(pInfo.pos);
    }

    public boolean isNodePage() {
        if (pInfo.page != null)
            return pInfo.page.isNode();
        else
            return PageUtils.isNodePage(pInfo.pos);
    }

    public int getBuffMemory() {
        return pInfo.getBuffMemory();
    }

    public long getLastTime() {
        return pInfo.lastTime;
    }

    public int getHits() {
        return pInfo.hits;
    }

    public void resetHits() {
        pInfo.hits = 0;
    }

    public void updateTime() {
        pInfo.updateTime();
    }

    public void releaseBuff() {
        pInfo.buff = null;
    }

    public void releasePage() {
        pInfo.page = null;
    }

    @Override
    public String toString() {
        return "PageReference[" + pInfo.pos + "]";
    }
}
