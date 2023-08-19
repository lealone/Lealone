/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.util.ArrayList;
import java.util.Collections;
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
        gcPages(now, 15 * 60 * 1000, 0, te); // 15+分钟都没再访问过，释放page字段和buff字段
        if (memoryManager.needGc())
            gcPages(now, 5 * 60 * 1000, 1, te); // 5+分钟都没再访问过，释放page字段保留buff字段
        if (memoryManager.needGc())
            gcPages(now, -2, 0, te); // 全表扫描的场景，释放page字段和buff字段
        if (memoryManager.needGc())
            lru(te, memoryManager); // 按LRU算法回收
        long size2 = memoryManager.getUsedMemory();
        memoryManager.resetUsedMemory();
        if (DEBUG)
            System.out.println("Map: " + map.getName() + ", GC: " + size1 + " -> " + size2);
    }

    // gcType: 0释放page和buff、1释放page、2释放buff
    private void gcPages(long now, long hitsOrIdleTime, int gcType, TransactionEngine te) {
        PageReference ref = map.getRootPageRef();
        gcPages(ref, now, hitsOrIdleTime, gcType, te);
    }

    private void gcPages(PageReference ref, long now, long hitsOrIdleTime, int gcType,
            TransactionEngine te) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, childRef -> {
                gcPages(childRef, now, hitsOrIdleTime, gcType, te);
            });
        }
        if (ref.canGc(te)) {
            boolean gc = false;
            if (hitsOrIdleTime < 0) {
                int hits = (int) -hitsOrIdleTime;
                if (pInfo.getHits() < hits) {
                    gc = true;
                }
            } else if (now - pInfo.getLastTime() > hitsOrIdleTime) {
                gc = true;
            }
            if (gc) {
                ref.gcPage(pInfo, gcType);
            }
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
            return (int) (getLastTime() - o.getLastTime());
        }
    }

    private void lru(TransactionEngine te, MemoryManager memoryManager) {
        ArrayList<GcingPage> list = new ArrayList<>();
        collect(list, map.getRootPageRef(), te);
        Collections.sort(list);
        release(list, 1); // 先释放page
        if (memoryManager.needGc())
            release(list, 2); // 如果内存依然紧张再释放buff
    }

    private void collect(ArrayList<GcingPage> list, PageReference ref, TransactionEngine te) {
        PageInfo pInfo = ref.getPageInfo();
        Page p = pInfo.page;
        if (p != null && p.isNode()) {
            forEachPage(p, childRef -> {
                collect(list, childRef, te);
            });
        }
        if (ref.canGc(te)) {
            list.add(new GcingPage(ref, pInfo));
        }
    }

    private void release(ArrayList<GcingPage> list, int gcType) {
        int size = list.size() / 2 + 1;
        for (GcingPage gp : list) {
            PageInfo pInfoNew = gp.ref.gcPage(gp.pInfo, gcType);
            if (pInfoNew != null)
                gp.pInfo = pInfoNew;
            if (size-- == 0)
                break;
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
