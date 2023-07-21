/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.util.Comparator;
import java.util.TreeSet;

import org.lealone.db.MemoryManager;
import org.lealone.storage.aose.btree.page.Page;
import org.lealone.storage.aose.btree.page.PageInfo;
import org.lealone.storage.aose.btree.page.PageReference;

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

    public void gc() {
        long now = System.currentTimeMillis();
        MemoryManager globalMemoryManager = GMM();
        gc(now, 30 * 60 * 1000, true);
        if (globalMemoryManager.needGc())
            gc(now, 15 * 60 * 1000, true);
        if (globalMemoryManager.needGc())
            gc(now, 5 * 60 * 1000, false);
        if (globalMemoryManager.needGc())
            gc(now, -2, true); // 全表扫描的场景
        if (globalMemoryManager.needGc())
            lru();
    }

    private TreeSet<PageInfo> lru() {
        Comparator<PageInfo> comparator = (pi1, pi2) -> (int) (pi1.getLastTime() - pi2.getLastTime());
        TreeSet<PageInfo> set = new TreeSet<>(comparator);
        collect(set, map.getRootPageRef().getPageInfo());
        release(set, true);
        if (GMM().needGc())
            release(set, false);
        return set;
    }

    private void collect(TreeSet<PageInfo> set, PageInfo pInfo) {
        Page p = pInfo.page;
        if (p == null)
            return;
        if (p.isNode()) {
            PageReference[] children = p.getChildren();
            for (int i = 0, len = children.length; i < len; i++) {
                PageReference ref = children[i];
                if (ref != null) {
                    collect(set, ref.getPageInfo());
                }
            }
        } else {
            if (p.getPos() > 0 && p.markType != 1 && p.canGC()) // pos为0时说明page被修改了，不能回收
                set.add(pInfo);
        }
    }

    private void release(TreeSet<PageInfo> set, boolean releasePage) {
        MemoryManager globalMemoryManager = GMM();
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
            memoryManager.decrementUsedMemory(memory);
            globalMemoryManager.decrementUsedMemory(memory);
            if (size-- == 0)
                break;
        }
    }

    private void gc(long now, long hitsOrIdleTime, boolean gcAll) {
        gc(map.getRootPageRef().getPageInfo(), now, hitsOrIdleTime, gcAll);
    }

    private void gc(PageInfo pInfo, long now, long hitsOrIdleTime, boolean gcAll) {
        Page p = pInfo.page;
        if (p == null)
            return;
        if (p.isNode()) {
            PageReference[] children = p.getChildren();
            for (int i = 0, len = children.length; i < len; i++) {
                PageReference ref = children[i];
                if (ref != null) {
                    gc(ref.getPageInfo(), now, hitsOrIdleTime, gcAll);
                }
            }
        } else {
            gcLeafPage(pInfo, p, now, hitsOrIdleTime, gcAll);
        }
    }

    private void gcLeafPage(PageInfo pInfo, Page p, long now, long hitsOrIdleTime, boolean gcAll) {
        if (p.getPos() == 0) // pos为0时说明page被修改了，不能回收
            return;
        if (p.getRef().isLocked()) // 其他事务准备更新page，所以没必要回收
            return;
        if (p.markType > 0 && !p.canGC())
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
            memoryManager.decrementUsedMemory(memory);
            GMM().decrementUsedMemory(memory);
        }
    }
}
