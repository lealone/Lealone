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
    }

    public void close() {
        addUsedMemory(-memoryManager.getUsedMemory());
    }

    public void gc() {
        collect(false);
    }

    public void fullGc() {
        collect(true);
    }

    private boolean needGc() {
        return memoryManager.needGc();
    }

    private void collect(boolean fullGc) {
        if (fullGc || needGc())
            new Collector().collect(fullGc);
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

    private class Collector {
        // 按层级收集node page，单独收集leaf page
        private final ArrayList<GcingPage> leafPages = new ArrayList<>();
        private final HashMap<Integer, ArrayList<GcingPage>> nodePageMap = new HashMap<>();
        private final long now = System.currentTimeMillis();
        private boolean fullGc;

        private void collect(boolean fullGc) {
            this.fullGc = fullGc;
            collect(map.getRootPageRef(), 1, new AtomicBoolean(true));
            // 在collect内部也会快速收集掉一些特殊的page，如果不需要进一步收集了就直接返回
            if (fullGc || !needGc())
                return;
            // 接下来按LRU算法释放leaf page和node page
            // leaf page按LastTime从小到大释放
            releaseLeafPages(leafPages);
            // 从最下层的node page开始释放
            releaseNodePages(nodePageMap);
        }

        private void collect(PageReference ref, int level, AtomicBoolean gcParentNodePage) {
            PageInfo pInfo = ref.getPageInfo();
            Page p = pInfo.page;
            if (p != null && p.isNode()) {
                final AtomicBoolean gcNodePage = new AtomicBoolean(true);
                forEachPage(p, false, childRef -> {
                    collect(childRef, level + 1, gcNodePage);
                });
                if (ref.canGc() && gcNodePage.get()) {
                    gcPage(ref, level, gcParentNodePage, gcNodePage, pInfo, false);
                } else {
                    gcParentNodePage.set(false);
                }
            } else { // 也可能是buff字段不为null的NodePage
                if (ref.canGc()) {
                    gcPage(ref, level, gcParentNodePage, null, pInfo, ref.isLeafPage());
                } else {
                    gcParentNodePage.set(false);
                }
            }
        }

        private void gcPage(PageReference ref, int level, AtomicBoolean gcParentNodePage,
                AtomicBoolean gcNodePage, PageInfo pInfo, boolean isLeaf) {
            if (fullGc) {
                if (ref.gcPage(pInfo, 0) == null)
                    gcParentNodePage.set(false);
            } else {
                long interval = now - pInfo.getLastTime();
                if (interval > 300000 // 超过5分钟(5 * 60 * 1000)都没再访问过
                        || pInfo.getHits() < 2 && interval > 1000) { // 全表扫描的场景
                    if (ref.gcPage(pInfo, 0) == null)
                        gcParentNodePage.set(false);
                } else {
                    if (isLeaf) {
                        leafPages.add(new GcingPage(ref, pInfo, null, gcParentNodePage));
                    } else {
                        ArrayList<GcingPage> nodePages = nodePageMap.get(level);
                        if (nodePages == null) {
                            nodePages = new ArrayList<>();
                            nodePageMap.put(level, nodePages);
                        }
                        nodePages.add(new GcingPage(ref, pInfo, gcNodePage, gcParentNodePage));
                    }
                }
            }
        }

        private void releaseLeafPages(ArrayList<GcingPage> leafPages) {
            int size = leafPages.size();
            if (size == 0)
                return;
            Collections.sort(leafPages);

            int index = size / 2 + 1;
            // 先释放前一半的page字段和buff字段
            release(leafPages, 0, index, 0);
            // 再释放后一半
            if (needGc()) {
                release(leafPages, index, size, 1); // 先释放page字段
                if (needGc())
                    release(leafPages, index, size, 2); // 如果内存依然紧张再释放buff字段
            }
        }

        private void releaseNodePages(HashMap<Integer, ArrayList<GcingPage>> nodePageMap) {
            if (!nodePageMap.isEmpty() && needGc()) {
                ArrayList<Integer> levelList = new ArrayList<>(nodePageMap.keySet());
                Collections.sort(levelList);
                for (int i = levelList.size() - 1; i >= 0; i--) {
                    ArrayList<GcingPage> nodePages = nodePageMap.get(levelList.get(i));
                    int size = nodePages.size();
                    if (size > 0) {
                        release(nodePages, 0, size, 1); // 先释放page字段
                        if (needGc())
                            release(nodePages, 0, size, 2); // 如果内存依然紧张再释放buff字段
                    }
                    if (!needGc())
                        break;
                }
            }
        }

        private void release(ArrayList<GcingPage> list, int startIndex, int endIndex, int gcType) {
            for (int i = startIndex; i < endIndex; i++) {
                GcingPage gp = list.get(i);
                if (gp.ref.isNodePage() && (gp.gcNodePage == null || !gp.gcNodePage.get())) {
                    gp.gcParentNodePage.set(false);
                    continue;
                }
                PageInfo pInfoOld = gp.pInfo;
                PageInfo pInfoNew = gp.ref.gcPage(pInfoOld, gcType);
                if (pInfoNew != null) {
                    if (pInfoOld != pInfoNew)
                        gp.pInfo = pInfoNew;
                } else {
                    if (gp.gcNodePage != null)
                        gp.gcNodePage.set(false);
                    gp.gcParentNodePage.set(false);
                }
            }
        }
    }

    private static class GcingPage implements Comparable<GcingPage> {
        final PageReference ref;
        PageInfo pInfo; // 会被修改
        final AtomicBoolean gcNodePage;
        final AtomicBoolean gcParentNodePage;
        final long lastTime;

        GcingPage(PageReference ref, PageInfo pInfo, AtomicBoolean gcNodePage,
                AtomicBoolean gcParentNodePage) {
            this.ref = ref;
            this.pInfo = pInfo;
            this.gcNodePage = gcNodePage;
            this.gcParentNodePage = gcParentNodePage;
            this.lastTime = pInfo.getLastTime();
        }

        @Override
        public int compareTo(GcingPage o) {
            // 不能用pInfo.getLastTime()，否则可能抛异常: Comparison method violates its general contract!
            return Long.compare(lastTime, o.lastTime);
        }
    }
}
