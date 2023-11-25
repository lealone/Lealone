/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.db.MemoryManager;
import org.lealone.db.async.AsyncPeriodicTask;
import org.lealone.db.link.LinkableList;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.storage.StorageMap;
import org.lealone.storage.fs.FileStorage;
import org.lealone.transaction.TransactionEngine.GcTask;
import org.lealone.transaction.aote.log.RedoLogRecord;
import org.lealone.transaction.aote.log.RedoLogRecord.PendingCheckpoint;

public class CheckpointService implements MemoryManager.MemoryListener {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    private final AOTransactionEngine aote;

    private final LinkableList<PendingCheckpoint> pendingCheckpoints = new LinkableList<>();

    // 关闭CheckpointService时等待它结束
    private CountDownLatch latchOnClose;

    private final long dirtyPageCacheSize;
    private final long checkpointPeriod;
    private final long loopInterval;

    private long lastSavedAt = System.currentTimeMillis();
    private volatile boolean isClosed;

    private final CopyOnWriteArrayList<Runnable> forceCheckpointTasks = new CopyOnWriteArrayList<>();

    private final ConcurrentHashMap<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<GcTask> gcTasks = new CopyOnWriteArrayList<>();

    private final Scheduler scheduler;
    private AsyncPeriodicTask periodicTask;

    CheckpointService(AOTransactionEngine aoTransactionEngine, Map<String, String> config,
            int schedulerId) {
        aote = aoTransactionEngine;
        scheduler = aote.schedulerFactory.getScheduler(schedulerId);
        // 默认32M
        dirtyPageCacheSize = MapUtils.getLongMB(config, "dirty_page_cache_size_in_mb", 32 * 1024 * 1024);
        // 默认1小时
        checkpointPeriod = MapUtils.getLong(config, "checkpoint_period", 1 * 60 * 60 * 1000);
        // 默认3秒钟
        long loopInterval = MapUtils.getLong(config, "checkpoint_service_loop_interval", 3 * 1000);
        if (checkpointPeriod < loopInterval)
            loopInterval = checkpointPeriod;
        this.loopInterval = loopInterval;

        if (isMasterScheduler())
            MemoryManager.setGlobalMemoryListener(this);
    }

    public long getLoopInterval() {
        return loopInterval;
    }

    @Override
    public void wakeUp() {
        scheduler.wakeUp();
    }

    public void setAsyncPeriodicTask(AsyncPeriodicTask task) {
        this.periodicTask = task;
    }

    public AsyncPeriodicTask getAsyncPeriodicTask() {
        return periodicTask;
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

    private void close() {
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

    public void executeCheckpointOnClose(CountDownLatch latch) {
        if (isClosed) {
            latch.countDown();
            return;
        }
        latchOnClose = latch;
        forceCheckpointTasks.add(() -> executeCheckpoint(true));
        wakeUp();
    }

    private boolean isMasterScheduler() {
        return scheduler.getId() == 0;
    }

    // 按周期自动触发
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
            // 关闭后确保再执行一次保存点
            if (!pendingCheckpoints.isEmpty()) {
                gcPendingCheckpoints();
            }
        }
    }

    public void fullGc() {
        for (StorageMap<?, ?> map : maps.values()) {
            if (!map.isClosed()) {
                map.fullGc(aote);
            }
        }
    }

    private void gc() {
        // 只由master负责事务引擎的垃圾收集问题
        if (isMasterScheduler())
            aote.gcTValues();
        executeGcTasks();
        gcMaps();
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
                map.gc(aote);
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
            PendingCheckpoint pc = pendingCheckpoints.getHead();
            if (pc != null && !pc.isSynced()) {
                return; // 前一个检查点第一阶段没有完成就不生成第二个检查点
            }
            if (!forceCheckpointTasks.isEmpty()) {
                ArrayList<Runnable> tasks = new ArrayList<>(forceCheckpointTasks);
                forceCheckpointTasks.removeAll(tasks);
                for (Runnable task : tasks)
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
        long logId = aote.logSyncService.nextLogId();
        addPendingCheckpoint(logId, false, force);
    }

    // 第2步，logSyncService线程完成redo log chunk文件切换后会得到完成通知，此时由master发起一个checkpoint任务
    private void prepareCheckpointTask(PendingCheckpoint pc) {
        int schedulerCount = aote.schedulerFactory.getSchedulerCount();
        checkpointTask = new CheckpointTask(pc, scheduler, schedulerCount);
        for (int i = 0; i < schedulerCount; i++) {
            if (i != scheduler.getId()) {
                CheckpointService slave = aote.checkpointServices[i];
                slave.checkpointTask = checkpointTask;
                slave.scheduler.wakeUp();
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
        // 如果当前的checkpoint任务是关闭系统时发出的，关闭所有检查点服务
        if (latchOnClose != null) {
            close();
            latchOnClose.countDown();
        }
    }

    public PendingCheckpoint getPendingCheckpoint() {
        return pendingCheckpoints.getHead();
    }

    private void addPendingCheckpoint(long logId, boolean saved, boolean force) {
        PendingCheckpoint pc = RedoLogRecord.createPendingCheckpoint(logId, saved, force);
        pendingCheckpoints.add(pc);
        aote.logSyncService.asyncWakeUp();
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
                long dm = map.collectDirtyMemory(aote, usedMemory);
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
        private final Scheduler masterScheduler;
        private final AtomicInteger prepared;
        private boolean saved;

        public CheckpointTask(PendingCheckpoint pc, Scheduler masterScheduler, int schedulerCount) {
            this.pc = pc;
            this.masterScheduler = masterScheduler;
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
                    aote.logSyncService.getRedoLog().addFsyncTask(task);
                    aote.logSyncService.wakeUp();
                } else {
                    onSynced(syncedCount, checkpointTask);
                }
            }
            lastSavedAt = now;
        } catch (Throwable t) {
            logger.error("Failed to execute checkpoint", t);
            aote.logSyncService.getRedoLog().ignoreCheckpoint();
        } finally {
            scheduler.setFsyncDisabled(false);
        }
    }

    private static void onSynced(AtomicInteger syncedCount, CheckpointTask checkpointTask) {
        if (syncedCount.decrementAndGet() == 0) {
            checkpointTask.prepared.decrementAndGet();
            checkpointTask.masterScheduler.wakeUp();
        }
    }
}
