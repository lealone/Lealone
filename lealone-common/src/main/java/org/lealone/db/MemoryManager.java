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

    private long maxMemory;
    private final AtomicLong usedMemory = new AtomicLong(0);
    private final AtomicLong dirtyMemory = new AtomicLong(0);

    public MemoryManager(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public long getUsedMemory() {
        return usedMemory.get();
    }

    public long getDirtyMemory() {
        return dirtyMemory.get();
    }

    public void decrementUsedMemory(long delta) {
        usedMemory.addAndGet(-delta);
    }

    public void addDirtyMemory(long delta) { // 正负都有可能
        dirtyMemory.addAndGet(delta);
    }

    public void addUsedMemory(long delta) { // 正负都有可能
        usedMemory.addAndGet(delta);
    }

    public void addUsedAndDirtyMemory(long delta) {
        usedMemory.addAndGet(delta);
        dirtyMemory.addAndGet(delta);
    }

    public boolean needGc(long delta) {
        return maxMemory > 0 && usedMemory.get() + delta > (maxMemory / 10 * 5);
    }

    public boolean needGc() {
        return maxMemory > 0 && usedMemory.get() > (maxMemory / 10 * 5);
    }

    public void reset() {
        usedMemory.set(0);
        dirtyMemory.set(0);
    }

    public void resetDirtyMemory() {
        dirtyMemory.set(0);
    }
}
