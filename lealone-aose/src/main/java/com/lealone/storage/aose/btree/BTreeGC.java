/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.lealone.db.MemoryManager;
import com.lealone.storage.aose.btree.page.Page;
import com.lealone.storage.aose.btree.page.PageInfo;
import com.lealone.storage.aose.btree.page.PageReference;

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

    public void addUsedMemory(long delta) {
        memoryManager.addUsedMemory(delta);
        GMM().addUsedMemory(delta);
        if (delta > 0 && needGc()) {
            MemoryManager.wakeUpGlobalMemoryListeners();
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

    public long collectDirtyMemory() {
        PageReference ref = map.getRootPageRef();
        if (!ref.getPageInfo().isDirty())
            return 0;
        AtomicLong dirtyMemory = new AtomicLong();
        collectDirtyMemory(ref, dirtyMemory);
        return dirtyMemory.get();
    }

    private void collectDirtyMemory(PageReference ref, AtomicLong dirtyMemory) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, true, childRef -> {
                collectDirtyMemory(childRef, dirtyMemory);
            });
        }
        dirtyMemory.addAndGet(pInfo.getPageMemory());
    }

    private void gc(MemoryManager memoryManager) {
        if (!memoryManager.needGc())
            return;
        long used = memoryManager.getUsedMemory();
        gcPages(15 * 60 * 1000, 0); // 15+分钟都没再访问过，释放page字段和buff字段
        if (memoryManager.needGc())
            gcPages(5 * 60 * 1000, 1); // 5+分钟都没再访问过，释放page字段保留buff字段
        if (memoryManager.needGc())
            gcPages(-2, 0); // 全表扫描的场景，释放page字段和buff字段
        if (memoryManager.needGc())
            lru(memoryManager); // 按LRU算法回收
        if (DEBUG) {
            System.out.println(
                    "Map: " + map.getName() + ", GC: " + used + " -> " + memoryManager.getUsedMemory());
        }
    }

    // gcType: 0释放page和buff、1释放page、2释放buff
    private void gcPages(long hitsOrIdleTime, int gcType) {
        gcPages(System.currentTimeMillis(), hitsOrIdleTime, gcType, map.getRootPageRef());
    }

    private void gcPages(long now, long hitsOrIdleTime, int gcType, PageReference ref) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, false, childRef -> {
                gcPages(now, hitsOrIdleTime, gcType, childRef);
            });
        }
        if (hitsOrIdleTime < 0) {
            if (pInfo.getHits() < -hitsOrIdleTime && ref.canGc()) {
                ref.gcPage(pInfo, gcType);
            }
        } else if (now - pInfo.getLastTime() > hitsOrIdleTime && ref.canGc()) {
            ref.gcPage(pInfo, gcType);
        }
    }

    private static class GcingPage implements Comparable<GcingPage> {
        PageReference ref;
        PageInfo pInfo;
        AtomicBoolean gcNodePage;
        AtomicBoolean gcParentNodePage;

        GcingPage(PageReference ref, PageInfo pInfo, AtomicBoolean gcNodePage,
                AtomicBoolean gcParentNodePage) {
            this.ref = ref;
            this.pInfo = pInfo;
            this.gcNodePage = gcNodePage;
            this.gcParentNodePage = gcParentNodePage;
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
        collect(map.getRootPageRef(), leafPages, nodePageMap, 1, new AtomicBoolean(true),
                new AtomicBoolean(true));

        // leaf page按LastTime从小到大释放
        releaseLeafPages(leafPages, memoryManager);

        // 从最下层的node page开始释放
        releaseNodePages(nodePageMap, memoryManager);
    }

    private void collect(PageReference ref, ArrayList<GcingPage> leafPages,
            HashMap<Integer, ArrayList<GcingPage>> nodePageMap, int level, AtomicBoolean gcNodePage,
            AtomicBoolean gcParentNodePage) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            final AtomicBoolean gc = new AtomicBoolean(true);
            forEachPage(p, false, childRef -> {
                collect(childRef, leafPages, nodePageMap, level + 1, gc, gcNodePage);
            });
        }
        if (ref.isNodePage()) {
            if (ref.canGc()) {
                ArrayList<GcingPage> nodePages = nodePageMap.get(level);
                if (nodePages == null) {
                    nodePages = new ArrayList<>();
                    nodePageMap.put(level, nodePages);
                }
                nodePages.add(new GcingPage(ref, pInfo, gcNodePage, gcParentNodePage));
            }
        } else {
            if (ref.canGc())
                leafPages.add(new GcingPage(ref, pInfo, gcNodePage, gcParentNodePage));
        }
        if (pInfo.isDirty()) {
            gcNodePage.set(false);
            gcParentNodePage.set(false);
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
            PageInfo pInfoOld = gp.pInfo;
            if (gp.ref.isNodePage() && !gp.gcNodePage.get()) {
                gp.gcParentNodePage.set(false);
                continue;
            }
            PageInfo pInfoNew = gp.ref.gcPage(pInfoOld, gcType);
            if (pInfoNew != null) {
                if (pInfoOld != pInfoNew)
                    gp.pInfo = pInfoNew;
            } else {
                gp.gcNodePage.set(false);
                gp.gcParentNodePage.set(false);
            }
        }
    }

    private void forEachPage(Page p, boolean collectDirtyPage, Consumer<PageReference> action) {
        PageReference[] children = p.getChildren();
        for (int i = 0, len = children.length; i < len; i++) {
            PageReference childRef = children[i];
            if (childRef != null) {
                if (collectDirtyPage) {
                    if (childRef.getPageInfo().isDirty())
                        action.accept(childRef);
                } else if (childRef.getPageInfo().isOnline()) { // 离线page不用GC
                    action.accept(childRef);
                }
            }
        }
    }
}
