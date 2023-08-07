/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.lealone.db.MemoryManager;
import org.lealone.db.session.Session;
import org.lealone.storage.aose.btree.page.Page;
import org.lealone.storage.aose.btree.page.PageInfo;
import org.lealone.storage.aose.btree.page.PageReference;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;

public class BTreeGC {

    private static MemoryManager GMM() {
        return MemoryManager.getGlobalMemoryManager();
    }

    private final BTreeMap<?, ?> map;
    private final MemoryManager memoryManager;

    public BTreeGC(BTreeMap<?, ?> map, long maxMemory) {
        this.map = map;
        if (maxMemory <= 0)
            maxMemory = GMM().getMaxMemory();
        memoryManager = new MemoryManager(maxMemory);
    }

    public long getMaxMemory() {
        return memoryManager.getMaxMemory();
    }

    public void setMaxMemory(long mb) {
        memoryManager.setMaxMemory(mb * 1024 * 1024);
    }

    public long getUsedMemory() {
        return memoryManager.getUsedMemory();
    }

    public long getDirtyMemory() {
        return memoryManager.getDirtyMemory();
    }

    public void addDirtyMemory(long delta) {
        memoryManager.addDirtyMemory(delta);
        GMM().addDirtyMemory(delta);
    }

    public void addUsedMemory(long delta) {
        memoryManager.addUsedMemory(delta);
        GMM().addUsedMemory(delta);
    }

    public void addUsedAndDirtyMemory(long delta) {
        memoryManager.addUsedAndDirtyMemory(delta);
        GMM().addUsedAndDirtyMemory(delta);
    }

    public void resetDirtyMemory() {
        long mem = memoryManager.getDirtyMemory();
        memoryManager.resetDirtyMemory();
        GMM().addDirtyMemory(-mem);
    }

    public void close() {
        GMM().addDirtyMemory(-memoryManager.getDirtyMemory());
        GMM().addUsedMemory(-memoryManager.getUsedMemory());
        memoryManager.reset();
    }

    public void gc(TransactionEngine te) {
        MemoryManager globalMemoryManager = GMM();
        if (!globalMemoryManager.needGc()) {
            if (!map.getRootPageRef().getTids().isEmpty())
                gcTids(te);
            return;
        }
        long now = System.currentTimeMillis();
        gc(now, 15 * 60 * 1000, true, te);
        if (globalMemoryManager.needGc())
            gc(now, 5 * 60 * 1000, false, te);
        if (globalMemoryManager.needGc())
            gc(now, -2, true, te); // 全表扫描的场景
        if (globalMemoryManager.needGc())
            lru(te);
    }

    private TreeSet<PageInfo> lru(TransactionEngine te) {
        Comparator<PageInfo> comparator = (pi1, pi2) -> (int) (pi1.getLastTime() - pi2.getLastTime());
        TreeSet<PageInfo> set = new TreeSet<>(comparator);
        collect(set, map.getRootPageRef().getPageInfo(), te);
        release(set, true);
        if (GMM().needGc())
            release(set, false);
        return set;
    }

    private void collect(TreeSet<PageInfo> set, PageInfo pInfo, TransactionEngine te) {
        Page p = pInfo.page;
        if (p == null)
            return;
        if (p.isNode()) {
            PageReference[] children = p.getChildren();
            for (int i = 0, len = children.length; i < len; i++) {
                PageReference ref = children[i];
                if (ref != null) {
                    collect(set, ref.getPageInfo(), te);
                }
            }
        }
        if (p.getPos() > 0 && canGC(p, te)) // pos为0时说明page被修改了，不能回收
            set.add(pInfo);
    }

    private void release(TreeSet<PageInfo> set, boolean releasePage) {
        int size = set.size() / 3 + 1;
        for (PageInfo pInfo : set) {
            long memory;
            if (releasePage) {
                Page p = pInfo.page;
                if (p == null)
                    continue;
                memory = p.getMemory();
                pInfo.releasePage();
            } else {
                memory = pInfo.getBuffMemory();
                pInfo.releaseBuff();
            }
            addUsedMemory(-memory);
            if (size-- == 0)
                break;
        }
    }

    private void gc(long now, long hitsOrIdleTime, boolean gcAll, TransactionEngine te) {
        gc(map.getRootPageRef().getPageInfo(), now, hitsOrIdleTime, gcAll, te);
    }

    private void gc(PageInfo pInfo, long now, long hitsOrIdleTime, boolean gcAll, TransactionEngine te) {
        Page p = pInfo.page;
        if (p == null)
            return;
        if (p.isNode()) {
            PageReference[] children = p.getChildren();
            for (int i = 0, len = children.length; i < len; i++) {
                PageReference ref = children[i];
                if (ref != null) {
                    gc(ref.getPageInfo(), now, hitsOrIdleTime, gcAll, te);
                }
            }
        }
        gcPage(pInfo, p, now, hitsOrIdleTime, gcAll, te);
    }

    private void gcPage(PageInfo pInfo, Page p, long now, long hitsOrIdleTime, boolean gcAll,
            TransactionEngine te) {
        if (p.getPos() == 0) // pos为0时说明page被修改了，不能回收
            return;
        if (p.getRef().isLocked()) // 其他事务准备更新page，所以没必要回收
            return;
        if (!canGC(p, te))
            return;
        boolean gc = false;
        if (hitsOrIdleTime < 0) {
            int hits = (int) -hitsOrIdleTime;
            if (pInfo.getHits() < hits) {
                gc = true;
                pInfo.resetHits();
            }
        } else if (now - pInfo.getLastTime() > hitsOrIdleTime) {
            gc = true;
        }
        if (gc) {
            long memory;
            if (gcAll)
                memory = p.getTotalMemory();
            else
                memory = p.getMemory();
            pInfo.releasePage();
            if (gcAll)
                pInfo.releaseBuff();
            addUsedMemory(-memory);
        }
    }

    private boolean canGC(Page p, TransactionEngine te) {
        if (te == null)
            return true;
        ConcurrentSkipListSet<Long> tids = gcTids(p, te);
        boolean isReadOnly = true;
        for (Long tid : tids) {
            Transaction t = te.getTransaction(tid);
            if (t != null) {
                Session s = t.getSession();
                if (s != null && s.isForUpdate()) {
                    isReadOnly = false;
                    break;
                }
            }
        }
        return tids.isEmpty() || isReadOnly;
    }

    private void gcTids(TransactionEngine te) {
        gcTids(map.getRootPageRef().getPageInfo(), te);
    }

    private void gcTids(PageInfo pInfo, TransactionEngine te) {
        Page p = pInfo.page;
        if (p == null)
            return;
        if (p.isNode()) {
            PageReference[] children = p.getChildren();
            for (int i = 0, len = children.length; i < len; i++) {
                PageReference ref = children[i];
                if (ref != null) {
                    gcTids(ref.getPageInfo(), te);
                }
            }
        }
        gcTids(p, te);
    }

    private ConcurrentSkipListSet<Long> gcTids(Page p, TransactionEngine te) {
        ConcurrentSkipListSet<Long> tids = p.getRef().getTids();
        if (te == null) {
            tids.clear();
            return tids;
        }
        for (Long tid : tids) {
            if (!te.containsTransaction(tid)) {
                tids.remove(tid);
                addUsedMemory(-32);
            }
        }
        return tids;
    }
}
