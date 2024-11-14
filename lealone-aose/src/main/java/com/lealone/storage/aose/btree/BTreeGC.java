/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.lealone.common.util.SystemPropertyUtils;
import com.lealone.db.MemoryManager;
import com.lealone.storage.aose.btree.page.Page;
import com.lealone.storage.aose.btree.page.PageInfo;
import com.lealone.storage.aose.btree.page.PageReference;

public class BTreeGC {

    public static final boolean DEBUG = false;

    public static final boolean gcNodePages = SystemPropertyUtils
            .getBoolean("lealone.memory.gcNodePages", false);

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

    public void addUsedMemory(long delta) {
        memoryManager.addUsedMemory(delta);
        GMM().addUsedMemory(delta);
        if (delta > 0 && needGc()) {
            MemoryManager.wakeUpGlobalMemoryListener();
        }
    }

    public void close() {
        addUsedMemory(-memoryManager.getUsedMemory());
    }

    public boolean needGc() {
        return memoryManager.needGc();
    }

    public void fullGc() {
        memoryManager.forceGc(true);
        try {
            gc();
        } finally {
            memoryManager.forceGc(false);
        }
    }

    public void gc() {
        gc(memoryManager);
    }

    public long collectDirtyMemory(AtomicLong usedMemory) {
        AtomicLong dirtyMemory = new AtomicLong();
        gcPages(15 * 60 * 1000, 0, dirtyMemory, usedMemory); // 15+分钟都没再访问过，释放page字段和buff字段
        return dirtyMemory.get();
    }

    private void gc(MemoryManager memoryManager) {
        if (!memoryManager.needGc())
            return;
        long used = memoryManager.getUsedMemory();
        // gcPages(15 * 60 * 1000, 0, null, null); // 15+分钟都没再访问过，释放page字段和buff字段
        if (memoryManager.needGc())
            gcPages(5 * 60 * 1000, 1, null, null); // 5+分钟都没再访问过，释放page字段保留buff字段
        if (memoryManager.needGc())
            gcPages(-2, 0, null, null); // 全表扫描的场景，释放page字段和buff字段
        if (memoryManager.needGc())
            lru(memoryManager); // 按LRU算法回收
        if (DEBUG) {
            System.out.println(
                    "Map: " + map.getName() + ", GC: " + used + " -> " + memoryManager.getUsedMemory());
        }
    }

    // gcType: 0释放page和buff、1释放page、2释放buff
    private void gcPages(long hitsOrIdleTime, int gcType, AtomicLong dirtyMemory,
            AtomicLong usedMemory) {
        gcPages(System.currentTimeMillis(), hitsOrIdleTime, gcType, map.getRootPageRef(), dirtyMemory,
                usedMemory);
    }

    private void gcPages(long now, long hitsOrIdleTime, int gcType, PageReference ref,
            AtomicLong dirtyMemory, AtomicLong usedMemory) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, childRef -> {
                gcPages(now, hitsOrIdleTime, gcType, childRef, dirtyMemory, usedMemory);
            });
        }
        if (hitsOrIdleTime < 0) {
            if (pInfo.getHits() < -hitsOrIdleTime && ref.canGc()) {
                ref.gcPage(pInfo, gcType);
            }
        } else if (now - pInfo.getLastTime() > hitsOrIdleTime && ref.canGc()) {
            ref.gcPage(pInfo, gcType);
        }
        if (dirtyMemory != null && pInfo.getPos() == 0) {
            dirtyMemory.addAndGet(pInfo.getPageMemory());
        }
        if (usedMemory != null) {
            usedMemory.addAndGet(pInfo.getTotalMemory());
        }
    }

    private static class GcingPage implements Comparable<GcingPage> {
        PageReference ref;
        PageInfo pInfo;

        GcingPage(PageReference ref, PageInfo pInfo) {
            this.ref = ref;
            this.pInfo = pInfo;
        }

        long getLastTime() {
            return pInfo.getLastTime();
        }

        @Override
        public int compareTo(GcingPage o) {
            // 不能直接相减，否则可能抛异常: Comparison method violates its general contract!
            return Long.compare(getLastTime(), o.getLastTime());
        }
    }

    private void lru(MemoryManager memoryManager) {
        // 按层级收集node page，单独收集leaf page
        HashMap<Integer, ArrayList<GcingPage>> nodePageMap = new HashMap<>();
        ArrayList<GcingPage> leafPages = new ArrayList<>();
        collect(map.getRootPageRef(), leafPages, nodePageMap, 1);

        // leaf page按LastTime从小到大释放
        releaseLeafPages(leafPages, memoryManager);

        // 从最下层的node page开始释放
        if (gcNodePages)
            releaseNodePages(nodePageMap, memoryManager);
    }

    private void collect(PageReference ref, ArrayList<GcingPage> leafPages,
            HashMap<Integer, ArrayList<GcingPage>> nodePageMap, int level) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, childRef -> {
                collect(childRef, leafPages, nodePageMap, level + 1);
            });
        }
        if (ref.canGc()) {
            if (ref.isNodePage()) {
                if (gcNodePages) {
                    ArrayList<GcingPage> nodePages = nodePageMap.get(level);
                    if (nodePages == null) {
                        nodePages = new ArrayList<>();
                        nodePageMap.put(level, nodePages);
                    }
                    nodePages.add(new GcingPage(ref, pInfo));
                }
            } else {
                leafPages.add(new GcingPage(ref, pInfo));
            }
        }
    }

    private void releaseLeafPages(ArrayList<GcingPage> leafPages, MemoryManager memoryManager) {
        int size = leafPages.size();
        if (size == 0)
            return;
        Collections.sort(leafPages);

        int index = size / 2 + 1;
        // 先释放前一半的page字段和buff字段
        release(leafPages, 0, index, 0);
        // 再释放后一半
        if (memoryManager.needGc()) {
            release(leafPages, index, size, 1); // 先释放page字段
            if (memoryManager.needGc())
                release(leafPages, index, size, 2); // 如果内存依然紧张再释放buff字段
        }
    }

    private void releaseNodePages(HashMap<Integer, ArrayList<GcingPage>> nodePageMap,
            MemoryManager memoryManager) {
        if (!nodePageMap.isEmpty() && memoryManager.needGc()) {
            ArrayList<Integer> levelList = new ArrayList<>(nodePageMap.keySet());
            Collections.sort(levelList);
            for (int i = levelList.size() - 1; i >= 0; i--) {
                ArrayList<GcingPage> nodePages = nodePageMap.get(levelList.get(i));
                int size = nodePages.size();
                if (size > 0) {
                    release(nodePages, 0, size, 1); // 先释放page字段
                    if (memoryManager.needGc())
                        release(nodePages, 0, size, 2); // 如果内存依然紧张再释放buff字段
                }
                if (!memoryManager.needGc())
                    break;
            }
        }
    }

    private void release(ArrayList<GcingPage> list, int startIndex, int endIndex, int gcType) {
        for (int i = startIndex; i < endIndex; i++) {
            GcingPage gp = list.get(i);
            PageInfo pInfoNew = gp.ref.gcPage(gp.pInfo, gcType);
            if (pInfoNew != null)
                gp.pInfo = pInfoNew;
        }
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
