/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.log;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.MapUtils;
import com.lealone.db.MemoryManager;
import com.lealone.db.RunMode;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.util.Awaiter;
import com.lealone.transaction.PendingTransaction;
import com.lealone.transaction.aote.AOTransaction;
import com.lealone.transaction.aote.CheckpointService;

public abstract class LogSyncService extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(LogSyncService.class);

    public static final String LOG_SYNC_TYPE_PERIODIC = "periodic";
    public static final String LOG_SYNC_TYPE_INSTANT = "instant";
    public static final String LOG_SYNC_TYPE_NO_SYNC = "no_sync";

    private final Awaiter awaiter = new Awaiter(logger);
    private final AtomicLong asyncLogQueueSize = new AtomicLong();
    private final AtomicLong lastLogId = new AtomicLong();

    private final InternalScheduler[] waitingSchedulers;

    // 只要达到一定的阈值就可以立即同步了
    private final int redoLogRecordSyncThreshold;
    private final RedoLog redoLog;

    private volatile boolean running;
    private volatile CountDownLatch latchOnClose;

    protected volatile long lastSyncedAt = System.currentTimeMillis();
    protected long syncIntervalMillis;

    private CheckpointService checkpointService;

    public LogSyncService(Map<String, String> config) {
        setName(getClass().getSimpleName());
        setDaemon(RunMode.isEmbedded(config));
        int schedulerCount = MapUtils.getSchedulerCount(config);
        waitingSchedulers = new InternalScheduler[schedulerCount];
        redoLogRecordSyncThreshold = MapUtils.getInt(config, "redo_log_record_sync_threshold", 100);
        redoLog = new RedoLog(config, this);
    }

    public void setCheckpointService(CheckpointService checkpointService) {
        this.checkpointService = checkpointService;
    }

    public CheckpointService getCheckpointService() {
        return checkpointService;
    }

    public RedoLog getRedoLog() {
        return redoLog;
    }

    public long nextLogId() {
        return lastLogId.incrementAndGet();
    }

    public AtomicLong getAsyncLogQueueSize() {
        return asyncLogQueueSize;
    }

    public InternalScheduler[] getWaitingSchedulers() {
        return waitingSchedulers;
    }

    public boolean needSync() {
        return true;
    }

    public boolean isPeriodic() {
        return false;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void run() {
        running = true;
        long lastCheckedAt = System.currentTimeMillis();
        while (running) {
            long syncStarted = System.currentTimeMillis();
            sync();
            redoLog.runPendingTransactions();
            lastSyncedAt = syncStarted;
            if (!isPeriodic()) {
                // 如果是instant sync，只要一有redo log就接着马上同步，无需等待
                if (asyncLogQueueSize.get() > 0)
                    continue;
            } else if (asyncLogQueueSize.get() > redoLogRecordSyncThreshold) {
                // 如果是periodic sync，只要redo log达到阈值也接着马上同步，无需等待
                continue;
            }
            if (MemoryManager.needFullGc())
                checkpointService.fullGc();
            long now = System.currentTimeMillis();
            if (checkpointService.forceCheckpoint()
                    || lastCheckedAt + checkpointService.getLoopInterval() < now) {
                if (!redoLog.hasPendingTransactions())
                    checkpointService.run();
                redoLog.clearIdleBuffers(now);
                lastCheckedAt = now;
            }
            long sleep = syncStarted + syncIntervalMillis - now;
            if (sleep < 0)
                continue;
            awaiter.doAwait(sleep);
        }
        // 结束前最后sync一次
        sync();
        while (true) {
            redoLog.runPendingTransactions();
            if (!redoLog.hasPendingTransactions()) {
                checkpointService.run();
                break;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }
        if (latchOnClose != null) {
            latchOnClose.countDown();
        }
    }

    private void sync() {
        try {
            if (asyncLogQueueSize.get() > 0)
                redoLog.save();
        } catch (Exception e) {
            logger.error("Failed to sync redo log", e);
        }
    }

    // 调用join可能没有效果，run方法可能在main线程中运行，所以统一用CountDownLatch
    public void close() {
        latchOnClose = new CountDownLatch(1);
        running = false;
        wakeUp();
        try {
            latchOnClose.await();
        } catch (InterruptedException e) {
        }
        // 放在最后关闭
        checkpointService.close();
    }

    public void wakeUp() {
        awaiter.wakeUp();
    }

    private void wakeUp(InternalScheduler scheduler) {
        waitingSchedulers[scheduler.getId()] = scheduler;
        asyncLogQueueSize.getAndIncrement();
        wakeUp();
    }

    public abstract void asyncWrite(AOTransaction t, RedoLogRecord r, long logId);

    public abstract void syncWrite(AOTransaction t, RedoLogRecord r, long logId);

    private static void addPendingTransaction(PendingTransaction pt, AOTransaction t) {
        InternalScheduler scheduler = pt.getScheduler();
        scheduler.addPendingTransaction(pt);
        LogSyncService[] logSyncServices = t.transactionEngine.getLogSyncServices();
        Set<Integer> serviceIndexs = t.getUndoLog().getRedoLogServiceIndexs();
        for (int i : serviceIndexs) {
            // 不用写RedoLog的内存表直接返回
            if (i < 0 && serviceIndexs.size() == 1) {
                pt.setSynced(true);
                return;
            }
            logSyncServices[i].wakeUp(scheduler);
        }
    }

    public static LogSyncService create(Map<String, String> config) {
        LogSyncService logSyncService;
        String logSyncType = config.get("log_sync_type");
        if (logSyncType == null || LOG_SYNC_TYPE_PERIODIC.equalsIgnoreCase(logSyncType))
            logSyncService = new Periodic(config);
        else if (LOG_SYNC_TYPE_INSTANT.equalsIgnoreCase(logSyncType))
            logSyncService = new Instant(config);
        else if (LOG_SYNC_TYPE_NO_SYNC.equalsIgnoreCase(logSyncType))
            logSyncService = new NoSync(config);
        else
            throw new IllegalArgumentException("Unknow log_sync_type: " + logSyncType);
        return logSyncService;
    }

    private static class NoSync extends LogSyncService {

        NoSync(Map<String, String> config) {
            super(config);
        }

        @Override
        public boolean needSync() {
            return false;
        }

        @Override
        public void run() {
        }

        @Override
        public void close() {
        }

        @Override
        public void asyncWrite(AOTransaction t, RedoLogRecord r, long logId) {
            t.onSynced();
            t.asyncCommitComplete();
        }

        @Override
        public void syncWrite(AOTransaction t, RedoLogRecord r, long logId) {
            t.onSynced();
        }
    }

    // 事务需要等到数据fsync到硬盘才能给客户端发回响应消息
    private static class Instant extends LogSyncService {

        Instant(Map<String, String> config) {
            super(config);
            // 只是一个睡眠时间，新事务有RedoLog要写就会唤醒它去写
            syncIntervalMillis = MapUtils.getLong(config, "log_sync_service_loop_interval", 100);
        }

        @Override
        public void asyncWrite(AOTransaction t, RedoLogRecord r, long logId) {
            addPendingTransaction(new PendingTransaction(t, r, logId), t);
        }

        @Override // 会阻塞当前事务直到数据fsync到硬盘
        public void syncWrite(AOTransaction t, RedoLogRecord r, long logId) {
            CountDownLatch latch = new CountDownLatch(1);
            PendingTransaction pt = new PendingTransaction(t, r, logId);
            pt.setCompleted(true);
            pt.setLatch(latch);
            addPendingTransaction(pt, t);
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw DbException.convert(e);
            }
        }
    }

    // 事务不需要等到数据fsync到硬盘就能提前给客户端发回响应消息
    private static class Periodic extends LogSyncService {

        Periodic(Map<String, String> config) {
            super(config);
            // 这个参数只是为了兼容老版本
            // 并不是每隔一段时间同步一次，只是一个睡眠时间，新事务有RedoLog要写就会唤醒它去写
            syncIntervalMillis = MapUtils.getLong(config, "log_sync_period", 500);
        }

        @Override
        public boolean isPeriodic() {
            return true;
        }

        @Override
        public void asyncWrite(AOTransaction t, RedoLogRecord r, long logId) {
            write(t, r, logId, true);
        }

        @Override // 跟asyncWrite一样，并不会阻塞当前事务，只是不用调用asyncCommitComplete()因为上层负责调用它
        public void syncWrite(AOTransaction t, RedoLogRecord r, long logId) {
            write(t, r, logId, false);
        }

        private void write(AOTransaction t, RedoLogRecord r, long logId, boolean isAsync) {
            PendingTransaction pt = new PendingTransaction(t, r, logId);
            t.onSynced(); // 不能直接pt.setSynced(true);
            pt.setCompleted(true);
            addPendingTransaction(pt, t);
            if (isAsync)
                t.asyncCommitComplete();
        }
    }
}
