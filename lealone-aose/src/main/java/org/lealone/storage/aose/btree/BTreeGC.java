/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.lealone.db.MemoryManager;
import org.lealone.db.session.Session;
import org.lealone.storage.aose.btree.page.Page;
import org.lealone.storage.aose.btree.page.PageInfo;
import org.lealone.storage.aose.btree.page.PageReference;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;

public class BTreeGC {

    public static final boolean DEBUG = false;

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
        if (delta > 0 && needGc())
            MemoryManager.wakeUpGlobalMemoryListener();
    }

    public void addUsedMemory(long delta) {
        memoryManager.addUsedMemory(delta);
        GMM().addUsedMemory(delta);
        if (delta > 0 && needGc())
            MemoryManager.wakeUpGlobalMemoryListener();
    }

    public void addUsedAndDirtyMemory(long delta) {
        memoryManager.addUsedAndDirtyMemory(delta);
        GMM().addUsedAndDirtyMemory(delta);
        if (delta > 0 && needGc())
            MemoryManager.wakeUpGlobalMemoryListener();
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

    public boolean needGc() {
        return memoryManager.needGc();
    }

    public void gc(TransactionEngine te) {
        gc(te, memoryManager);
    }

    public void gcGlobal(TransactionEngine te) {
        MemoryManager globalMemoryManager = GMM();
        gc(te, globalMemoryManager);
    }

    private void gc(TransactionEngine te, MemoryManager memoryManager) {
        if (!memoryManager.needGc())
            return;
        long size1 = memoryManager.getUsedMemory();
        long now = System.currentTimeMillis();
        gcPages(now, 15 * 60 * 1000, true, te); // 15+分钟都没再访问过，释放page字段和buff字段
        if (memoryManager.needGc())
            gcPages(now, 5 * 60 * 1000, false, te); // 5+分钟都没再访问过，释放page字段保留buff字段
        if (memoryManager.needGc())
            gcPages(now, -2, true, te); // 全表扫描的场景，释放page字段和buff字段
        if (memoryManager.needGc())
            lru(te, memoryManager); // 按LRU算法回收
        long size2 = memoryManager.getUsedMemory();
        if (DEBUG)
            System.out.println("Map: " + map.getName() + ", GC: " + size1 + " -> " + size2);
    }

    private void gcPages(long now, long hitsOrIdleTime, boolean gcAll, TransactionEngine te) {
        PageReference ref = map.getRootPageRef();
        gcPages(ref, ref.getPageInfo(), now, hitsOrIdleTime, gcAll, te);
    }

    private void gcPages(PageReference ref, PageInfo pInfo, long now, long hitsOrIdleTime, boolean gcAll,
            TransactionEngine te) {
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, childRef -> {
                gcPages(childRef, childRef.getPageInfo(), now, hitsOrIdleTime, gcAll, te);
            });
        }
        if (canGC(ref, pInfo, p, te)) {
            gcPage(ref, pInfo, p, now, hitsOrIdleTime, gcAll);
        }
    }

    private void gcPage(PageReference ref, PageInfo pInfo, Page p, long now, long hitsOrIdleTime,
            boolean gcAll) {
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
            PageInfo pInfoNew = pInfo.copy(true);
            long memory;
            if (p != null) {
                if (gcAll)
                    memory = p.getTotalMemory();
                else
                    memory = p.getMemory();
                pInfoNew.releasePage();
            } else {
                memory = pInfo.getBuffMemory();
            }
            if (gcAll)
                pInfoNew.releaseBuff();
            if (ref.replacePage(pInfo, pInfoNew))
                addUsedMemory(-memory);
        }
    }

    private static class GcingPage {
        PageReference ref;
        PageInfo pInfo;

        GcingPage(PageReference ref, PageInfo pInfo) {
            this.ref = ref;
            this.pInfo = pInfo;
        }

        long getLastTime() {
            return pInfo.getLastTime();
        }
    }

    private void lru(TransactionEngine te, MemoryManager memoryManager) {
        Comparator<GcingPage> comparator = (p1, p2) -> (int) (p1.getLastTime() - p2.getLastTime());
        TreeSet<GcingPage> set = new TreeSet<>(comparator);
        collect(set, map.getRootPageRef(), te);
        release(set, true); // 先释放page
        if (memoryManager.needGc())
            release(set, false); // 如果内存依然紧张再释放buff
    }

    private void collect(TreeSet<GcingPage> set, PageReference ref, TransactionEngine te) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, childRef -> {
                collect(set, childRef, te);
            });
        }
        if (canGC(ref, pInfo, p, te)) {
            set.add(new GcingPage(ref, pInfo));
        }
    }

    private void release(TreeSet<GcingPage> set, boolean releasePage) {
        int size = set.size() / 2 + 1;
        for (GcingPage gp : set) {
            PageInfo pInfo = gp.pInfo;
            PageReference ref = gp.ref;
            if (ref.getPageInfo() != pInfo) // 发生变动了，不需要进一步的操作
                continue;
            long memory;
            if (releasePage) {
                Page p = pInfo.page;
                if (p == null)
                    continue;
                PageInfo pInfoNew = pInfo.copy(true);
                pInfoNew.releasePage();
                memory = p.getMemory();
                if (ref.replacePage(pInfo, pInfoNew)) {
                    addUsedMemory(-memory);
                    gp.pInfo = pInfoNew;
                }
            } else {
                if (pInfo.buff == null)
                    continue;
                memory = pInfo.getBuffMemory();
                PageInfo pInfoNew = pInfo.copy(true);
                pInfo.releaseBuff();
                if (ref.replacePage(pInfo, pInfoNew))
                    addUsedMemory(-memory);
            }
            if (size-- == 0)
                break;
        }
    }

    private boolean canGC(PageReference ref, PageInfo pInfo, Page p, TransactionEngine te) {
        if (p == null && pInfo.buff == null)
            return false;
        if (p != null && p.getPos() == 0) // pos为0时说明page被修改了，不能回收
            return false;
        if (ref.isLocked()) // 其他事务准备更新page，所以没必要回收
            return false;
        if (te == null)
            return true;
        for (Transaction t : te.currentTransactions().values()) {
            Session s = t.getSession();
            if (s != null && s.containsPageReference(ref) && s.isForUpdate()) {
                return false;
            }
        }
        return true;
    }

    private void forEachPage(Page p, Consumer<PageReference> action) {
        PageReference[] children = p.getChildren();
        for (int i = 0, len = children.length; i < len; i++) {
            PageReference childRef = children[i];
            if (childRef != null) {
                action.accept(childRef);
            }
        }
    }
}
