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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.MapUtils;
import com.lealone.db.MemoryManager;
import com.lealone.db.async.AsyncPeriodicTask;
import com.lealone.db.link.LinkableList;
import com.lealone.db.lock.Lockable;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.storage.StorageMap;
import com.lealone.storage.fs.FileStorage;
import com.lealone.transaction.TransactionEngine.GcTask;
import com.lealone.transaction.aote.TransactionalValue.OldValue;
import com.lealone.transaction.aote.log.RedoLogRecord;
import com.lealone.transaction.aote.log.RedoLogRecord.PendingCheckpoint;

public class CheckpointService implements MemoryManager.MemoryListener, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    private final AOTransactionEngine aote;
    private final InternalScheduler scheduler;
    private final AsyncPeriodicTask periodicTask;
    private final long dirtyPageCacheSize;
    private final long checkpointPeriod;

    // 只有redo log sync线程读,checkpoint线程写
    private final LinkableList<PendingCheckpoint> pendingCheckpoints = new LinkableList<>();
    // 以下三个字段都是低频场景使用，会有多个线程执行add和remove
    private final CopyOnWriteArrayList<Runnable> forceCheckpointTasks = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<GcTask> gcTasks = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();

    private long lastSavedAt = System.currentTimeMillis();
    private volatile boolean isClosed;
    private volatile CountDownLatch latchOnClose;

    CheckpointService(AOTransactionEngine aote, Map<String, String> config,
            InternalScheduler scheduler) {
        this.aote = aote;
        this.scheduler = scheduler;
        // 默认32M
        dirtyPageCacheSize = MapUtils.getLongMB(config, "dirty_page_cache_size_in_mb", 32 * 1024 * 1024);
        // 默认1小时
        checkpointPeriod = MapUtils.getLong(config, "checkpoint_period", 1 * 60 * 60 * 1000);
        // 默认3秒钟
        long loopInterval = MapUtils.getLong(config, "checkpoint_service_loop_interval", 3 * 1000);
        if (checkpointPeriod < loopInterval)
            loopInterval = checkpointPeriod;

        periodicTask = new AsyncPeriodicTask(loopInterval, loopInterval, this);
        scheduler.addPeriodicTask(periodicTask);

        if (isMasterScheduler())
            MemoryManager.setGlobalMemoryListener(this);
    }

    private boolean isMasterScheduler() {
        return scheduler.getId() == 0;
    }

    @Override
    public void wakeUp() {
        periodicTask.resetLast(); // 马上执行，不用等到下一个周期
        scheduler.wakeUp();
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
        return scheduler.isStarted();
    }

    public void close() {
        if (!isClosed) {
            isClosed = true;
            int schedulerCount = aote.schedulerFactory.getSchedulerCount();
            for (int i = 0; i < schedulerCount; i++) {
                aote.checkpointServices[i].periodicTask.cancel();
            }
            if (isMasterScheduler())
                MemoryManager.setGlobalMemoryListener(null);
        }
    }

    // 例如通过执行CHECKPOINT语句触发
    public void executeCheckpointAsync() {
        if (isClosed)
            return;
        // 异步执行checkpoint命令
        forceCheckpointTasks.add(() -> executeCheckpoint(true));
        wakeUp();
    }

    public void executeCheckpointOnClose() {
        if (isClosed)
            return;
        latchOnClose = new CountDownLatch(1);
        forceCheckpointTasks.add(() -> executeCheckpoint(true));
        wakeUp();
        try {
            latchOnClose.await(3000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
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
        if (isMasterScheduler()) {
            // 关闭后确保再执行一次checkpoint
            if (!pendingCheckpoints.isEmpty()) {
                gcPendingCheckpoints();
            }
        }
    }

    public void fullGc() {
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
            lockable.getPageListener().getPageReference().remove(oldValue.key);
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
        for (StorageMap<?, ?> map : maps.values()) {
            if (!map.isClosed()) {
                map.gc();
            }
        }
    }

    private void executeCheckpoint() {
        if (checkpointTask != null) {
            if (executeCheckpointTask())
                return;
        }
        if (isMasterScheduler()) {
            gcPendingCheckpoints();
            if (!pendingCheckpoints.isEmpty() || checkpointTask != null)
                return; // 前一个检查点第一阶段没有完成就不生成第二个检查点
            if (!forceCheckpointTasks.isEmpty()) {
                // 每次只执行一个
                Runnable task = forceCheckpointTasks.remove(0);
                task.run();
            } else {
                executeCheckpoint(false);
            }
        } else {
            collectDirtyMemory(); // slave只需要收集脏页内存大小
        }
    }

    private void executeCheckpoint(boolean force) {
        collectDirtyMemory();
        long now = System.currentTimeMillis();
        boolean executeCheckpoint = force || isClosed || (lastSavedAt + checkpointPeriod < now);

        // 如果上面的条件都不满足，那么再看看已经提交的数据占用的预估总内存大小是否大于阈值
        if (!executeCheckpoint) {
            executeCheckpoint = dirtyMemoryTotal.get() > dirtyPageCacheSize;
        }
        if (executeCheckpoint && isMasterScheduler()) {
            switchRedoLogChunkFile(force);
        }
    }

    // 第1步，先切换redo log chunk文件，但是还没有写入一个checkpoint log
    private void switchRedoLogChunkFile(boolean force) {
        long logId = aote.getLogSyncService().nextLogId();
        addPendingCheckpoint(logId, false, force);
    }

    // 第2步，logSyncService线程完成redo log chunk文件切换后会得到完成通知，此时由master发起一个checkpoint任务
    private void prepareCheckpointTask(PendingCheckpoint pc) {
        int schedulerCount = aote.schedulerFactory.getSchedulerCount();
        checkpointTask = new CheckpointTask(pc, this, schedulerCount);
        for (int i = 0; i < schedulerCount; i++) {
            if (i != scheduler.getId()) {
                CheckpointService slave = aote.checkpointServices[i];
                slave.checkpointTask = checkpointTask;
                slave.wakeUp();
            }
        }
    }

    // 第3步，master和salve执行checkpoint任务
    private boolean executeCheckpointTask() {
        if (isMasterScheduler()) {
            if (!checkpointTask.isSaved()) {
                PendingCheckpoint pc = checkpointTask.pc;
                save(pc.getCheckpointId(), pc.isForce());
                checkpointTask.setSaved(true);
            }
            if (checkpointTask.isCompleted()) {
                commitCheckpointTask(checkpointTask.pc);
                return true;
            }
            return false;
        } else {
            collectDirtyMemory();
            save(-1, false); // 刷脏页
            checkpointTask = null;
            return true;
        }
    }

    // 第4步，master把checkpoint log提交logSyncService线程把checkpoint log写到上一个redo log chunk文件的末尾
    // 至此，整个checkpoint任务就完成了
    private void commitCheckpointTask(PendingCheckpoint pc) {
        // 把checkpoint对应的redo log放到最后那个chunk文件
        addPendingCheckpoint(pc.getCheckpointId(), true, pc.isForce());
        checkpointTask = null;
        if (latchOnClose != null) {
            latchOnClose.countDown();
        }
    }

    public PendingCheckpoint getPendingCheckpoint() {
        return pendingCheckpoints.getHead();
    }

    private void addPendingCheckpoint(long logId, boolean saved, boolean force) {
        // logger.info("logId：" + logId + ", saved: " + saved);
        PendingCheckpoint pc = RedoLogRecord.createPendingCheckpoint(logId, saved, force);
        pendingCheckpoints.add(pc);
        aote.getLogSyncService().asyncWakeUp();
    }

    private void gcPendingCheckpoints() {
        if (pendingCheckpoints.isEmpty())
            return;
        PendingCheckpoint pc = pendingCheckpoints.getHead();
        while (pc != null && pc.isSynced()) {
            if (!pc.isSaved()) {
                prepareCheckpointTask(pc);
                // master可以直接执行了
                executeCheckpointTask();
            }
            pc = pc.getNext();
            pendingCheckpoints.decrementSize();
            pendingCheckpoints.setHead(pc);
        }
        if (pendingCheckpoints.getHead() == null)
            pendingCheckpoints.setTail(null);
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

    private volatile CheckpointTask checkpointTask;

    private static class CheckpointTask {

        private final PendingCheckpoint pc;
        private final CheckpointService masterCheckpointService;
        private final AtomicInteger prepared;
        private boolean saved;

        public CheckpointTask(PendingCheckpoint pc, CheckpointService masterCheckpointService,
                int schedulerCount) {
            this.pc = pc;
            this.masterCheckpointService = masterCheckpointService;
            this.prepared = new AtomicInteger(schedulerCount);
        }

        boolean isCompleted() {
            return prepared.get() == 0;
        }

        boolean isSaved() {
            return saved;
        }

        void setSaved(boolean saved) {
            this.saved = saved;
        }
    }

    public static class FsyncTask {

        private final AtomicInteger syncedCount;
        private final CheckpointTask checkpointTask;
        private final FileStorage fsyncingFileStorage;

        public FsyncTask(AtomicInteger syncedCount, CheckpointTask checkpointTask,
                FileStorage fsyncingFileStorage) {
            this.syncedCount = syncedCount;
            this.checkpointTask = checkpointTask;
            this.fsyncingFileStorage = fsyncingFileStorage;
        }

        public void onSynced() {
            CheckpointService.onSynced(syncedCount, checkpointTask);
        }

        public FileStorage getFsyncingFileStorage() {
            return fsyncingFileStorage;
        }
    }

    // 把脏页刷到硬盘
    private void save(long logId, boolean force) {
        scheduler.setFsyncDisabled(true);
        long now = System.currentTimeMillis();
        try {
            AtomicInteger syncedCount = new AtomicInteger(maps.size());
            // 为0时什么都不需要做
            if (syncedCount.get() == 0) {
                onSynced(syncedCount, checkpointTask);
            } else {
                for (StorageMap<?, ?> map : maps.values()) {
                    if (map.isClosed()) {
                        onSynced(syncedCount, checkpointTask);
                        continue;
                    }
                    scheduler.setFsyncingFileStorage(null);
                    Long dirtyMemory = dirtyMaps.get(map.getName());
                    if (dirtyMemory != null)
                        map.save(dirtyMemory.longValue());
                    else if (force || map.hasUnsavedChanges())
                        map.save();
                    if (scheduler.getFsyncingFileStorage() != null) {
                        FsyncTask task = new FsyncTask(syncedCount, checkpointTask,
                                scheduler.getFsyncingFileStorage());
                        aote.getLogSyncService().getRedoLog().addFsyncTask(task);
                        aote.getLogSyncService().wakeUp();
                    } else {
                        onSynced(syncedCount, checkpointTask);
                    }
                }
            }
            lastSavedAt = now;
        } catch (Throwable t) {
            logger.error("Failed to execute checkpoint", t);
            aote.getLogSyncService().getRedoLog().ignoreCheckpoint();
        } finally {
            scheduler.setFsyncDisabled(false);
        }
    }

    private static void onSynced(AtomicInteger syncedCount, CheckpointTask checkpointTask) {
        if (syncedCount.decrementAndGet() <= 0) {
            checkpointTask.prepared.decrementAndGet();
            checkpointTask.masterCheckpointService.wakeUp();
        }
    }
}
