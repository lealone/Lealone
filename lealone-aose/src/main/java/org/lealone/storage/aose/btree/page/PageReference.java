/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.page.PageKey;
import org.lealone.storage.page.PageOperationHandler;

public class PageReference {

    private static final long REMOTE_PAGE_POS = -1;

    public static PageReference createRemotePageReference(Object key, boolean first) {
        return new PageReference(null, REMOTE_PAGE_POS, key, first);
    }

    static PageReference createRemotePageReference() {
        return new PageReference(null, REMOTE_PAGE_POS);
    }

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
        do {
            PageOperationHandler owner = lockOwner;
            boolean ok = lockUpdater.compareAndSet(this, null, newLockOwner);
            if (!ok && owner != null) {
                owner.addWaitingHandler(newLockOwner);
            }
            if (ok)
                return true;
        } while (lockOwner == null);
        return false;
    }

    public void unlock() {
        if (lockOwner != null) {
            PageOperationHandler owner = lockOwner;
            lockOwner = null;
            owner.wakeUpWaitingHandlers();
        }
    }

    Page page;
    PageKey pageKey;
    long pos;
    List<String> replicationHostIds;

    public PageReference() {
    }

    public PageReference(long pos) {
        this.pos = pos;
    }

    public PageReference(Page page, long pos) {
        this.page = page;
        this.pos = pos;
        if (page != null) {
            replicationHostIds = page.getReplicationHostIds();
        }
    }

    public PageReference(Page page) {
        this.page = page;
        if (page != null) {
            pos = page.getPos();
            replicationHostIds = page.getReplicationHostIds();
        }
    }

    PageReference(Page page, long pos, Object key, boolean first) {
        this(page, pos);
        setPageKey(key, first);
    }

    public PageReference(Page page, Object key, boolean first) {
        this(page);
        setPageKey(key, first);
    }

    public long getPos() {
        return pos;
    }

    public Page getPage() {
        return page;
    }

    public PageKey getPageKey() {
        return pageKey;
    }

    public List<String> getReplicationHostIds() {
        return replicationHostIds;
    }

    public void setReplicationHostIds(List<String> replicationHostIds) {
        this.replicationHostIds = replicationHostIds;
    }

    public void replacePage(Page page) {
        this.page = page;
        if (page != null) {
            pos = page.getPos();
            replicationHostIds = page.getReplicationHostIds();
        }
    }

    @Override
    public String toString() {
        return "PageReference[ pos=" + pos + "]";
    }

    void setPageKey(Object key, boolean first) {
        pageKey = new PageKey(key, first);
    }

    public boolean isRemotePage() {
        if (page != null)
            return page.isRemote();
        else
            return pos == REMOTE_PAGE_POS;
    }

    boolean isLeafPage() {
        if (page != null)
            return page.isLeaf();
        else
            return pos != REMOTE_PAGE_POS && PageUtils.isLeafPage(pos);
    }

    boolean isNodePage() {
        if (page != null)
            return page.isNode();
        else
            return pos != REMOTE_PAGE_POS && PageUtils.isNodePage(pos);
    }

    public synchronized Page readRemotePage(BTreeMap<?, ?> map) {
        return null;
    }
}
