/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.MapUtils;
import com.lealone.db.MemoryManager;
import com.lealone.db.lock.Lockable;
import com.lealone.storage.StorageMap;
import com.lealone.storage.page.IPageReference;
import com.lealone.transaction.TransactionEngine.GcTask;
import com.lealone.transaction.aote.TransactionalValue.OldValue;
import com.lealone.transaction.aote.log.LogSyncService;

public class CheckpointService implements MemoryManager.MemoryListener, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    private final AOTransactionEngine aote;
    private final LogSyncService logSyncService;
    private final long dirtyPageCacheSize;
    private final long checkpointPeriod;
    private final long loopInterval;

    // 以下三个字段都是低频场景使用，会有多个线程执行add和remove
    private final CopyOnWriteArrayList<Runnable> forceCheckpointTasks = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<GcTask> gcTasks = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();

    private long lastSavedAt = System.currentTimeMillis();
    private volatile boolean isClosed;

    public CheckpointService(AOTransactionEngine aote, Map<String, String> config,
            LogSyncService logSyncService) {
        this.aote = aote;
        this.logSyncService = logSyncService;

        // 默认32M
        dirtyPageCacheSize = MapUtils.getLongMB(config, "dirty_page_cache_size_in_mb", 32 * 1024 * 1024);
        // 默认1小时
        checkpointPeriod = MapUtils.getLong(config, "checkpoint_period", 1 * 60 * 60 * 1000);

        // 默认3秒钟
        long loopInterval = MapUtils.getLong(config, "checkpoint_service_loop_interval", 3 * 1000);
        if (checkpointPeriod < loopInterval)
            loopInterval = checkpointPeriod;
        this.loopInterval = loopInterval;

        MemoryManager.addGlobalMemoryListener(this);
    }

    public long getLoopInterval() {
        return loopInterval;
    }

    public boolean forceCheckpoint() {
        return !forceCheckpointTasks.isEmpty();
    }

    @Override
    public void wakeUp() {
        logSyncService.wakeUp();
    }

    public void addMap(StorageMap<?, ?> map) {
        maps.put(map.getName(), map);
    }

    public void removeMap(String mapName) {
        maps.remove(mapName);
    }

    public void addGcTask(GcTask gcTask) {
        gcTasks.add(gcTask);
    }

    public void removeGcTask(GcTask gcTask) {
        gcTasks.remove(gcTask);
    }

    public boolean isRunning() {
        return logSyncService.isRunning();
    }

    public void close() {
        if (!isClosed) {
            isClosed = true;
            MemoryManager.removeGlobalMemoryListener(this);
        }
    }

    // 例如通过执行CHECKPOINT语句触发,或者在关闭时触发
    public void executeCheckpointAsync() {
        if (isClosed)
            return;
        // 异步执行checkpoint命令
        forceCheckpointTasks.add(() -> executeCheckpoint(true));
        wakeUp();
    }

    // 按周期自动触发
    @Override
    public void run() {
        if (!isClosed) {
            try {
                gc();
            } catch (Throwable t) {
                logger.error("Failed to execute gc", t);
            }
            try {
                executeCheckpoint();
            } catch (Throwable t) {
                logger.error("Failed to execute checkpoint", t);
            }
        }
    }

    public void fullGc() {
        if (maps.isEmpty())
            return;
        for (StorageMap<?, ?> map : maps.values()) {
            if (!map.isClosed()) {
                map.fullGc();
            }
        }
    }

    private void gc() {
        gcTValues();
        executeGcTasks();
        gcMaps();
    }

    private void gcTValues() {
        if (maps.isEmpty())
            return;
        for (StorageMap<?, ?> map : maps.values()) {
            if (!map.isClosed()) {
                gcTValues(map);
            }
        }
    }

    private void gcTValues(StorageMap<?, ?> map) {
        ConcurrentHashMap<Lockable, Object> tValues = map.getOldValueCache();
        if (tValues.isEmpty())
            return;
        if (!aote.containsRepeatableReadTransactions()) {
            removeTValues(map, tValues);
            return;
        }
        long minTid = Long.MAX_VALUE;
        for (AOTransaction t : aote.currentTransactions()) {
            if (t.isRepeatableRead() && t.getTransactionId() < minTid)
                minTid = t.getTransactionId();
        }
        if (minTid != Long.MAX_VALUE) {
            for (Entry<Lockable, Object> e : tValues.entrySet()) {
                OldValue oldValue = (OldValue) e.getValue();
                if (oldValue != null && oldValue.tid < minTid) {
                    removeTValue(map, tValues, e.getKey(), oldValue);
                    continue;
                }
                while (oldValue != null) {
                    if (oldValue.tid < minTid) {
                        oldValue.next = null;
                        break;
                    }
                    oldValue = oldValue.next;
                }
            }
        } else {
            removeTValues(map, tValues);
        }
    }

    private void removeTValues(StorageMap<?, ?> map, ConcurrentHashMap<Lockable, Object> tValues) {
        for (Entry<Lockable, Object> e : tValues.entrySet()) {
            removeTValue(map, tValues, e.getKey(), (OldValue) e.getValue());
        }
    }

    private void removeTValue(StorageMap<?, ?> map, ConcurrentHashMap<Lockable, Object> tValues,
            Lockable lockable, OldValue oldValue) {
        if (lockable.getLockedValue() == null) {
            IPageReference ref = lockable.getPageListener().getPageReference();
            // 删除记录时会把lockable中的列置null，这里需要减去所有列占用的内存
            int memory = map.getValueType().getColumnsMemory(oldValue.value);
            if (memory != 0)
                ref.addPageUsedMemory(-memory);
            ref.remove(oldValue.key);
        }
        tValues.remove(lockable, oldValue); // 如果不是原来的就不删除
    }

    private void executeGcTasks() {
        if (gcTasks.isEmpty())
            return;
        for (GcTask gcTask : gcTasks) {
            gcTask.gc(aote);
        }
    }

    private void gcMaps() {
        if (maps.isEmpty())
            return;
        for (StorageMap<?, ?> map : maps.values()) {
            if (!map.isClosed()) {
                map.gc();
            }
        }
    }

    private void executeCheckpoint() {
        if (!forceCheckpointTasks.isEmpty()) {
            // 每次只执行一个
            Runnable task = forceCheckpointTasks.remove(0);
            task.run();
        } else {
            executeCheckpoint(false);
        }
    }

    private void executeCheckpoint(boolean force) {
        if (maps.isEmpty()) {
            return;
        }
        collectDirtyMemory();

        if (force // 强制刷脏页
                || isClosed // 关闭前要刷脏页
                || dirtyMemoryTotal.get() > dirtyPageCacheSize // 脏页占用的预估总内存大于阈值
                || lastSavedAt + checkpointPeriod < System.currentTimeMillis()) // 周期超过阈值了
        {
            try {
                if (!dirtyMaps.isEmpty()) {
                    for (Entry<String, Long> e : dirtyMaps.entrySet()) {
                        StorageMap<?, ?> map = maps.get(e.getKey());
                        // 准备耍脏页前如果表被删除了那就直接忽略
                        if (map != null && !map.isClosed())
                            map.save(e.getValue().longValue());
                    }
                    lastSavedAt = System.currentTimeMillis();
                }
            } catch (Throwable t) {
                logger.error("Failed to execute save", t);
            }
        }
    }

    private static final boolean DEBUG = false;
    private static final AtomicLong dirtyMemoryTotal = new AtomicLong();
    private final HashMap<String, Long> dirtyMaps = new HashMap<>();
    private final AtomicLong dirtyMemory = new AtomicLong();
    private long lastTime;

    private String toM(long v) {
        return v + "(" + (v >> 10) + "K)";
    }

    private void collectDirtyMemory() {
        dirtyMemoryTotal.addAndGet(-dirtyMemory.get());
        dirtyMaps.clear();
        dirtyMemory.set(0);

        AtomicLong usedMemory = null;
        if (DEBUG)
            usedMemory = new AtomicLong();
        for (StorageMap<?, ?> map : maps.values()) {
            if (!map.isClosed()) {
                long dm = map.collectDirtyMemory();
                if (DEBUG)
                    usedMemory.addAndGet(map.getMemorySpaceUsed());
                if (dm > 0) {
                    dirtyMemory.addAndGet(dm);
                    dirtyMaps.put(map.getName(), dm);
                }
            }
        }
        dirtyMemoryTotal.addAndGet(dirtyMemory.get());
        if (DEBUG) {
            if (System.currentTimeMillis() - lastTime < 3000)
                return;
            lastTime = System.currentTimeMillis();
            logger.info("Dirty maps: " + dirtyMaps);
            logger.info("DB g_used: " + toM(MemoryManager.getGlobalMemoryManager().getUsedMemory()));
            logger.info("DB c_used: " + toM(usedMemory.get()) + ", dirty: " + toM(dirtyMemory.get()));
            MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            logger.info("JVM used: " + toM(mu.getUsed()) + ", max: " + toM(mu.getMax()));
        }
    }
}
