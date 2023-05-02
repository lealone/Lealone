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

    public boolean tryLock(PageOperationHandler newLockOwner) {
        if (newLockOwner == lockOwner)
            return true;
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

    Page page;
    long pos;
    public PageInfo pInfo;

    public PageReference() {
    }

    public PageReference(long pos) {
        this.pos = pos;
    }

    public PageReference(Page page, long pos) {
        this.page = page;
        this.pos = pos;
        if (page != null)
            pInfo = page.pInfo;
    }

    public PageReference(Page page) {
        this.page = page;
        if (page != null) {
            pos = page.getPos();
            pInfo = page.pInfo;
        }
    }

    public long getPos() {
        return pos;
    }

    public Page getPage() {
        return page;
    }

    public void replacePage(Page page) {
        this.page = page;
        if (page != null) {
            pos = page.getPos();
            pInfo = page.pInfo;
        }
    }

    @Override
    public String toString() {
        return "PageReference[ pos=" + pos + "]";
    }

    public boolean isLeafPage() {
        if (page != null)
            return page.isLeaf();
        else
            return PageUtils.isLeafPage(pos);
    }

    public boolean isNodePage() {
        if (page != null)
            return page.isNode();
        else
            return PageUtils.isNodePage(pos);
    }

    public int getBuffMemory() {
        return pInfo == null ? 0 : pInfo.getBuffMemory();
    }

    public void clearBuff() {
        pInfo = null;
    }

    public long getLastTime() {
        if (page != null)
            return page.getPageInfo().lastTime;
        else
            return pInfo.lastTime;
    }

    public int getHits() {
        if (page != null)
            return page.getPageInfo().hits;
        else
            return pInfo.hits;
    }

    public void resetHits() {
        if (page != null)
            page.getPageInfo().hits = 0;
        else
            pInfo.hits = 0;
    }
}
