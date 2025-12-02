/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.log;

import java.util.Map;
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
import com.lealone.transaction.aote.AOTransactionEngine;
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

    private AOTransactionEngine engine;
    private CheckpointService checkpointService;

    private int syncServiceIndex;

    public LogSyncService(Map<String, String> config) {
        setName(getClass().getSimpleName());
        setDaemon(RunMode.isEmbedded(config));
        int schedulerCount = MapUtils.getSchedulerCount(config);
        waitingSchedulers = new InternalScheduler[schedulerCount];
        redoLogRecordSyncThreshold = MapUtils.getInt(config, "redo_log_record_sync_threshold", 100);
        redoLog = new RedoLog(config, this);
    }

    public int getSyncServiceIndex() {
        return syncServiceIndex;
    }

    public void setSyncServiceIndex(int syncServiceIndex) {
        this.syncServiceIndex = syncServiceIndex;
    }

    public void setEngine(AOTransactionEngine engine) {
        this.engine = engine;
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
            redoLog.save();
        } catch (Exception e) {
            logger.error("Failed to sync redo log", e);
        }
    }

    public void wakeUp() {
        awaiter.wakeUp();
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

    public void asyncWrite(AOTransaction t, RedoLogRecord r, long logId) {
        asyncWrite(new PendingTransaction(t, r, logId), t);
    }

    protected void asyncWrite(PendingTransaction pt, AOTransaction t) {
        InternalScheduler scheduler = pt.getScheduler();
        scheduler.addPendingTransaction(pt);
        LogSyncService[] logSyncServices = engine.getLogSyncServices();
        for (int i : t.getUndoLog().getRedoLogServiceIndexs()) {
            logSyncServices[i].wakeUp(scheduler);
        }
    }

    private void wakeUp(InternalScheduler scheduler) {
        waitingSchedulers[scheduler.getId()] = scheduler;
        asyncLogQueueSize.getAndIncrement();
        wakeUp();
    }

    public void syncWrite(AOTransaction t, RedoLogRecord r, long logId) {
        CountDownLatch latch = new CountDownLatch(1);
        PendingTransaction pt = new PendingTransaction(t, r, logId);
        pt.setCompleted(true);
        pt.setLatch(latch);
        asyncWrite(pt, t);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw DbException.convert(e);
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

    private static class Instant extends LogSyncService {

        Instant(Map<String, String> config) {
            super(config);
            syncIntervalMillis = MapUtils.getLong(config, "log_sync_service_loop_interval", 100);
        }
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

    private static class Periodic extends LogSyncService {

        private final long blockWhenSyncLagsMillis;

        Periodic(Map<String, String> config) {
            super(config);
            syncIntervalMillis = MapUtils.getLong(config, "log_sync_period", 500);
            blockWhenSyncLagsMillis = (long) (syncIntervalMillis * 1.5);
        }

        @Override
        public boolean isPeriodic() {
            return true;
        }

        private boolean waitForSyncToCatchUp() {
            // 如果当前时间是第10毫秒，上次同步时间是在第5毫秒，同步间隔是10毫秒，说时当前时间还是同步周期内，就不用阻塞了
            // 如果当前时间是第16毫秒，超过了同步周期，需要阻塞
            return System.currentTimeMillis() > lastSyncedAt + blockWhenSyncLagsMillis;
        }

        @Override
        public void asyncWrite(AOTransaction t, RedoLogRecord r, long logId) {
            PendingTransaction pt = new PendingTransaction(t, r, logId);
            // 如果在同步周期内，可以提前通知异步提交完成了
            if (!waitForSyncToCatchUp()) {
                t.onSynced(); // 不能直接pt.setSynced(true);
                pt.setCompleted(true);
                asyncWrite(pt, t);
                t.asyncCommitComplete();
            } else {
                asyncWrite(pt, t);
            }
        }

        @Override
        public void syncWrite(AOTransaction t, RedoLogRecord r, long logId) {
            // 如果在同步周期内，不用等
            if (!waitForSyncToCatchUp()) {
                PendingTransaction pt = new PendingTransaction(t, r, logId);
                t.onSynced();
                pt.setCompleted(true);
                // 同步调用无需t.asyncCommitComplete();
                asyncWrite(pt, t);
            } else {
                super.syncWrite(t, r, logId);
            }
        }
    }
}
