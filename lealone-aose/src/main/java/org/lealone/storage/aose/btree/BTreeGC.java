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
import org.lealone.storage.aose.btree.page.PageReference;

public class BTreeGC {

    private final BTreeMap<?, ?> map;
    private final MemoryManager memoryManager;

    public BTreeGC(BTreeMap<?, ?> map, long maxMemory) {
        this.map = map;
        if (maxMemory <= 0)
            maxMemory = MemoryManager.getGlobalMemoryManager().getMaxMemory();
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

    public void close() {
        MemoryManager.getGlobalMemoryManager().decrementMemory(memoryManager.getUsedMemory());
        memoryManager.reset();
    }

    public void gcIfNeeded(long delta) {
        MemoryManager globalMemoryManager = MemoryManager.getGlobalMemoryManager();
        if (globalMemoryManager.needGc()) {
            gc(false);
        }
        if (memoryManager.needGc(delta)) {
            long now = System.currentTimeMillis();
            gc(map.getRootPage(), now, -2, true); // 全表扫描的场景
            if (memoryManager.needGc(delta)) {
                TreeSet<PageReference> set = lru1();
                if (memoryManager.needGc(delta)) {
                    lru2(set);
                }
            }
        }
        memoryManager.incrementMemory(delta);
        globalMemoryManager.incrementMemory(delta);
    }

    public void gc() {
        gc(true);
    }

    private void gc(boolean lru) {
        long now = System.currentTimeMillis();
        MemoryManager globalMemoryManager = MemoryManager.getGlobalMemoryManager();
        gc(map.getRootPage(), now, 30 * 60 * 1000, true);
        if (globalMemoryManager.needGc())
            gc(map.getRootPage(), now, 15 * 60 * 1000, true);
        if (globalMemoryManager.needGc())
            gc(map.getRootPage(), now, 5 * 60 * 1000, false);
        if (globalMemoryManager.needGc())
            gc(map.getRootPage(), now, -2, true); // 全表扫描的场景
        if (lru && globalMemoryManager.needGc())
            lru2(lru1());
    }

    private TreeSet<PageReference> lru1() {
        MemoryManager globalMemoryManager = MemoryManager.getGlobalMemoryManager();
        Comparator<PageReference> comparator = (r1, r2) -> (int) (r1.getLastTime() - r2.getLastTime());
        TreeSet<PageReference> set = new TreeSet<>(comparator);
        collect(set, map.getRootPage());
        int size = set.size() / 3 + 1;
        for (PageReference ref : set) {
            Page p = ref.getPage();
            long memory = p.getMemory();
            ref.replacePage(null);
            memoryManager.decrementMemory(memory);
            globalMemoryManager.decrementMemory(memory);
            if (size-- == 0)
                break;
        }
        return set;
    }

    private void lru2(TreeSet<PageReference> set) {
        MemoryManager globalMemoryManager = MemoryManager.getGlobalMemoryManager();
        int size = set.size() / 3 + 1;
        for (PageReference ref : set) {
            long memory = ref.getBuffMemory();
            ref.clearBuff();
            memoryManager.decrementMemory(memory);
            globalMemoryManager.decrementMemory(memory);
            if (size-- == 0)
                break;
        }
    }

    private void collect(TreeSet<PageReference> set, Page parent) {
        if (parent.isNode()) {
            PageReference[] children = parent.getChildren();
            for (int i = 0, len = children.length; i < len; i++) {
                PageReference ref = children[i];
                if (ref != null) {
                    Page p = ref.getPage();
                    if (p != null && p.getPos() > 0) { // pos为0时说明page被修改了，不能回收
                        if (p.isNode()) {
                            collect(set, p);
                        } else {
                            set.add(ref);
                        }
                    }
                }
            }
        }
    }

    private void gc(Page parent, long now, long hitsOrIdleTime, boolean gcAll) {
        if (parent.isNode()) {
            PageReference[] children = parent.getChildren();
            for (int i = 0, len = children.length; i < len; i++) {
                PageReference ref = children[i];
                if (ref != null) {
                    Page p = ref.getPage();
                    if (p != null && p.getPos() > 0) { // pos为0时说明page被修改了，不能回收
                        if (p.isNode()) {
                            gc(p, now, hitsOrIdleTime, gcAll);
                        } else {
                            boolean gc = false;
                            if (hitsOrIdleTime < 0) {
                                int hits = (int) -hitsOrIdleTime;
                                if (ref.getHits() < hits) {
                                    gc = true;
                                    ref.resetHits();
                                }
                            } else if (now - ref.getLastTime() > hitsOrIdleTime) {
                                gc = true;
                            }
                            if (gc) {
                                long memory;
                                if (gcAll)
                                    memory = p.getTotalMemory();
                                else
                                    memory = p.getMemory();
                                ref.replacePage(null);
                                if (gcAll)
                                    ref.clearBuff();
                                memoryManager.decrementMemory(memory);
                                MemoryManager.getGlobalMemoryManager().decrementMemory(memory);
                            }
                        }
                    }
                }
            }
        }
    }
}
