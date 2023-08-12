/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryManager {

    private static final MemoryManager globalMemoryManager = new MemoryManager(getGlobalMaxMemory());

    private static long getGlobalMaxMemory() {
        MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        return mu.getMax();
    }

    public static MemoryManager getGlobalMemoryManager() {
        return globalMemoryManager;
    }

    public static interface MemoryListener {
        void wakeUp();
    }

    private static MemoryListener globalMemoryListener;

    public static void setGlobalMemoryListener(MemoryListener globalMemoryListener) {
        MemoryManager.globalMemoryListener = globalMemoryListener;
    }

    private static void wakeUpGlobalMemoryListener(long delta) {
        if (delta > 0 && globalMemoryListener != null && globalMemoryManager.needGc())
            globalMemoryListener.wakeUp();
    }

    public static void wakeUpGlobalMemoryListener() {
        if (globalMemoryListener != null)
            globalMemoryListener.wakeUp();
    }

    private final AtomicLong usedMemory = new AtomicLong(0);
    private final AtomicLong dirtyMemory = new AtomicLong(0);

    private long maxGCMemory;

    public MemoryManager(long maxMemory) {
        setMaxMemory(maxMemory);
    }

    public void setMaxMemory(long maxMemory) {
        if (maxMemory <= 0)
            maxMemory = getGlobalMaxMemory();
        maxGCMemory = maxMemory / 2; // 占用内存超过一半时就可以触发GC
    }

    public long getMaxMemory() {
        return maxGCMemory * 2;
    }

    public long getUsedMemory() {
        return usedMemory.get();
    }

    public long getDirtyMemory() {
        return dirtyMemory.get();
    }

    public void addDirtyMemory(long delta) { // 正负都有可能
        dirtyMemory.addAndGet(delta);
    }

    public void addUsedMemory(long delta) { // 正负都有可能
        usedMemory.addAndGet(delta);
        wakeUpGlobalMemoryListener(delta);
    }

    public void addUsedAndDirtyMemory(long delta) {
        usedMemory.addAndGet(delta);
        dirtyMemory.addAndGet(delta);
        wakeUpGlobalMemoryListener(delta);
    }

    public boolean needGc() {
        return usedMemory.get() > maxGCMemory;
    }

    public void reset() {
        usedMemory.set(0);
        dirtyMemory.set(0);
    }

    public void resetDirtyMemory() {
        dirtyMemory.set(0);
    }
}
