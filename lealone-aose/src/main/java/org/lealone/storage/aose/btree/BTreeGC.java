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
    private final MemoryManager memoryManager = new MemoryManager(-1);

    public BTreeGC(BTreeMap<?, ?> map, int maxMemory) {
        this.map = map;
        if (maxMemory > 0) {
            memoryManager.setMaxMemory(maxMemory);
        } else {
            memoryManager.setMaxMemory(MemoryManager.getGlobalMemoryManager().getMaxMemory());
        }
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

    public void reset() {
        memoryManager.reset();
    }

    public void gcIfNeeded(long delta) {
        if (memoryManager.needGc(delta)) {
            TreeSet<PageReference> set = gc1();
            if (memoryManager.needGc(delta)) {
                gc2(set);
            }
        }
        memoryManager.incrementMemory(delta);
        MemoryManager.getGlobalMemoryManager().incrementMemory(delta);
    }

    public void gc() {
        gc2(gc1());
    }

    private TreeSet<PageReference> gc1() {
        MemoryManager globalMemoryManager = MemoryManager.getGlobalMemoryManager();
        Comparator<PageReference> comparator = (r1, r2) -> (int) (r1.getLastTime() - r2.getLastTime());
        TreeSet<PageReference> set = new TreeSet<>(comparator);
        collect(set, map.getRootPage());
        int size = set.size() / 3 + 1;
        for (PageReference ref : set) {
            Page p = ref.getPage();
            long memory = p.getMemory();
            p.clear();
            ref.replacePage(null);
            memoryManager.decrementMemory(memory);
            globalMemoryManager.decrementMemory(memory);
            if (size-- == 0)
                break;
        }
        return set;
    }

    private void gc2(TreeSet<PageReference> set) {
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
}
