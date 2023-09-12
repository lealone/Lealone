/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.lealone.db.MemoryManager;
import org.lealone.storage.aose.btree.page.Page;
import org.lealone.storage.aose.btree.page.PageInfo;
import org.lealone.storage.aose.btree.page.PageReference;
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

    public void fullGc(TransactionEngine te) {
        memoryManager.forceGc(true);
        gc(te);
        memoryManager.forceGc(false);
    }

    public void gc(TransactionEngine te) {
        gc(te, memoryManager);
    }

    public long collectDirtyMemory(TransactionEngine te, AtomicLong usedMemory) {
        AtomicLong dirtyMemory = new AtomicLong();
        gcPages(te, 15 * 60 * 1000, 0, dirtyMemory, usedMemory); // 15+分钟都没再访问过，释放page字段和buff字段
        return dirtyMemory.get();
    }

    private void gc(TransactionEngine te, MemoryManager memoryManager) {
        if (!memoryManager.needGc())
            return;
        long used = memoryManager.getUsedMemory();
        // gcPages(te, 15 * 60 * 1000, 0, null, null); // 15+分钟都没再访问过，释放page字段和buff字段
        if (memoryManager.needGc())
            gcPages(te, 5 * 60 * 1000, 1, null, null); // 5+分钟都没再访问过，释放page字段保留buff字段
        if (memoryManager.needGc())
            gcPages(te, -2, 0, null, null); // 全表扫描的场景，释放page字段和buff字段
        if (memoryManager.needGc())
            lru(te, memoryManager); // 按LRU算法回收
        if (DEBUG) {
            System.out.println(
                    "Map: " + map.getName() + ", GC: " + used + " -> " + memoryManager.getUsedMemory());
        }
    }

    // gcType: 0释放page和buff、1释放page、2释放buff
    private void gcPages(TransactionEngine te, long hitsOrIdleTime, int gcType, AtomicLong dirtyMemory,
            AtomicLong usedMemory) {
        gcPages(te, System.currentTimeMillis(), hitsOrIdleTime, gcType, map.getRootPageRef(),
                dirtyMemory, usedMemory);
    }

    private void gcPages(TransactionEngine te, long now, long hitsOrIdleTime, int gcType,
            PageReference ref, AtomicLong dirtyMemory, AtomicLong usedMemory) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, childRef -> {
                gcPages(te, now, hitsOrIdleTime, gcType, childRef, dirtyMemory, usedMemory);
            });
        }
        if (hitsOrIdleTime < 0) {
            if (pInfo.getHits() < -hitsOrIdleTime && ref.canGc(te)) {
                ref.gcPage(pInfo, gcType);
            }
        } else if (now - pInfo.getLastTime() > hitsOrIdleTime && ref.canGc(te)) {
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

    private void lru(TransactionEngine te, MemoryManager memoryManager) {
        // 收集所有可以回收的page并按LastTime从小到大排序
        ArrayList<GcingPage> list = new ArrayList<>();
        collect(te, list, map.getRootPageRef());
        int size = list.size();
        if (size == 0)
            return;
        Collections.sort(list);

        int index = size / 2 + 1;
        // 先释放前一半的page字段和buff字段
        release(list, 0, index, 0);
        // 再释放后一半
        if (memoryManager.needGc()) {
            release(list, index, size, 1); // 先释放page字段
            if (memoryManager.needGc())
                release(list, index, size, 2); // 如果内存依然紧张再释放buff字段
        }
    }

    private void collect(TransactionEngine te, ArrayList<GcingPage> list, PageReference ref) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, childRef -> {
                collect(te, list, childRef);
            });
        }
        if (ref.canGc(te)) {
            list.add(new GcingPage(ref, pInfo));
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
