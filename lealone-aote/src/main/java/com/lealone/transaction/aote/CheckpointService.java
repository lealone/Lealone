/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.MapUtils;
import com.lealone.db.lock.Lockable;
import com.lealone.storage.StorageMap;
import com.lealone.storage.page.IPageReference;
import com.lealone.transaction.TransactionEngine.GcTask;
import com.lealone.transaction.aote.TransactionalValue.OldValue;
import com.lealone.transaction.aote.log.LogSyncService;

public class CheckpointService implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    private final AOTransactionEngine aote;
    private final LogSyncService logSyncService;
    private final long checkpointPeriod;
    private final long loopInterval;

    // 以下三个字段都是低频场景使用，会有多个线程执行add和remove
    private final CopyOnWriteArrayList<Runnable> forceCheckpointTasks = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<GcTask> gcTasks = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<StorageMap<?, ?>, AtomicLong> maps = new ConcurrentHashMap<>();

    private long lastSavedAt = System.currentTimeMillis();
    private volatile boolean isClosed;
    private volatile Thread workingThread; // 正在执行刷脏页或fullGc的线程

    public CheckpointService(AOTransactionEngine aote, Map<String, String> config,
            LogSyncService logSyncService) {
        this.aote = aote;
        this.logSyncService = logSyncService;

        // 默认12小时
        checkpointPeriod = MapUtils.getLong(config, "checkpoint_period", 12 * 60 * 60 * 1000);

        // 默认3秒钟
        long loopInterval = MapUtils.getLong(config, "checkpoint_service_loop_interval", 3 * 1000);
        if (checkpointPeriod < loopInterval)
            loopInterval = checkpointPeriod;
        this.loopInterval = loopInterval;
    }

    public long getLoopInterval() {
        return loopInterval;
    }

    public boolean hasForceCheckpoint() {
        return !forceCheckpointTasks.isEmpty();
    }

    public void addMap(StorageMap<?, ?> map) {
        maps.put(map, new AtomicLong(0));
    }

    public void removeMap(StorageMap<?, ?> map) {
        maps.remove(map);
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
        isClosed = true;
    }

    // 例如通过执行CHECKPOINT语句触发,或者在关闭时触发
    public void executeCheckpointAsync(boolean isClosing) {
        if (isClosed)
            return;
        // 异步执行checkpoint命令
        forceCheckpointTasks.add(() -> executeCheckpoint(true, isClosing));
        logSyncService.wakeUp(); // 唤醒执行检查点
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
        if (workingThread != null || maps.isEmpty())
            return;
        workingThread = new Thread(() -> {
            for (StorageMap<?, ?> map : maps.keySet()) {
                if (!map.isClosed()) {
                    workingThread.setName("FullGc-" + map.getName());
                    map.fullGc();
                }
            }
            workingThread = null;
        });
        workingThread.start();
    }

    private void gc() {
        gcTValues();
        executeGcTasks();
        gcMaps();
    }

    private void gcTValues() {
        if (maps.isEmpty())
            return;
        for (StorageMap<?, ?> map : maps.keySet()) {
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
        if (workingThread != null || maps.isEmpty())
            return;
        for (StorageMap<?, ?> map : maps.keySet()) {
            if (!map.isClosed()) {
                map.gc();
            }
        }
    }

    public void executeCheckpoint() {
        if (workingThread != null)
            return;
        if (!forceCheckpointTasks.isEmpty()) {
            // 每次只执行一个
            Runnable task = forceCheckpointTasks.remove(0);
            task.run();
        } else {
            executeCheckpoint(false, false);
        }
    }

    private void executeCheckpoint(boolean force, boolean isClosing) {
        if (maps.isEmpty())
            return;
        boolean needSave = collectDirtyMemory();

        if (force // 强制刷脏页
                || needSave // 有map的脏页占用的预估内存大于阈值
                || isClosed // 关闭前要刷脏页
                || lastSavedAt + checkpointPeriod < System.currentTimeMillis()) // 周期超过阈值了
        {
            if (isClosing) { // 正在关闭时，直接用当前线程保存
                save(force, true);
            } else {
                workingThread = new Thread(() -> {
                    save(force, false);
                    workingThread = null;
                });
                workingThread.start();
            }
        }
    }

    private boolean collectDirtyMemory() {
        boolean needSave = false;
        for (Entry<StorageMap<?, ?>, AtomicLong> e : maps.entrySet()) {
            StorageMap<?, ?> map = e.getKey();
            if (!map.isClosed()) {
                long size = map.collectDirtyMemory();
                e.getValue().set(size);
                if (size > 0 && size > map.getCacheSize())
                    needSave = true;
            }
        }
        return needSave;
    }

    private void save(boolean force, boolean isClosing) {
        long lastTransactionId = logSyncService.getRedoLog().getLastTransactionId();
        try {
            for (Entry<StorageMap<?, ?>, AtomicLong> e : maps.entrySet()) {
                StorageMap<?, ?> map = e.getKey();
                long size = e.getValue().get();
                // 准备耍刷页前如果表被删除了那就直接忽略
                if (size > 0 && !map.isClosed() && (force || size > map.getCacheSize())) {
                    long t1 = System.currentTimeMillis();
                    if (!isClosing)
                        workingThread.setName("Saving-" + map.getName());
                    map.setLastTransactionId(lastTransactionId);
                    try {
                        map.save(size);
                    } finally {
                        map.setLastTransactionId(-1);
                    }
                    if (logger.isDebugEnabled()) {
                        long time = System.currentTimeMillis() - t1;
                        logger.debug("Save {}, size: {}, time: {} ms", map.getName(), size, time);
                    }
                }
            }
            lastSavedAt = System.currentTimeMillis();
        } catch (Throwable t) {
            logger.error("Failed to execute save", t);
        }
    }
}
