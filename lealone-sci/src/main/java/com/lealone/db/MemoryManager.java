/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.util.SystemPropertyUtils;

public class MemoryManager {

    private static final MemoryManager globalMemoryManager = new MemoryManager(getGlobalMaxMemory());

    private static final long fullGcThreshold = getFullGcThreshold();

    public static boolean needFullGc() {
        return globalMemoryManager.getUsedMemory() > fullGcThreshold;
    }

    private static long getFullGcThreshold() {
        long max = getGlobalMaxMemory();
        long gcThreshold = max / 10 * 6;
        // 小于512M时把阈值调低一些
        if (max < 512 * 1024 * 1024)
            gcThreshold = max / 10 * 3;
        return SystemPropertyUtils.getLong("lealone.memory.fullGcThreshold", gcThreshold);
    }

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

    public static void wakeUpGlobalMemoryListener() {
        if (globalMemoryListener != null)
            globalMemoryListener.wakeUp();
    }

    private final AtomicLong usedMemory = new AtomicLong(0);
    private long gcThreshold;
    private boolean forceGc;

    public MemoryManager(long maxMemory) {
        setMaxMemory(maxMemory);
    }

    public void setMaxMemory(long maxMemory) {
        if (maxMemory <= 0)
            maxMemory = getGlobalMaxMemory();
        gcThreshold = maxMemory / 2; // 占用内存超过一半时就可以触发GC
    }

    public long getMaxMemory() {
        return gcThreshold * 2;
    }

    public long getUsedMemory() {
        return usedMemory.get();
    }

    public void addUsedMemory(long delta) { // 正负都有可能
        usedMemory.addAndGet(delta);
    }

    public boolean needGc() {
        if (forceGc)
            return true;
        if (usedMemory.get() > gcThreshold)
            return true;
        // 看看全部使用的内存是否超过阈值
        if (this != globalMemoryManager && globalMemoryManager.needGc())
            return true;
        return false;
    }

    public void forceGc(boolean b) {
        forceGc = b;
    }
}
