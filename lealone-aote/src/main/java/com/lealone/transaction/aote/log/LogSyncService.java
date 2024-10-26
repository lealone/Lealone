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
import com.lealone.db.RunMode;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.util.Awaiter;
import com.lealone.transaction.PendingTransaction;
import com.lealone.transaction.aote.AOTransaction;

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

    public LogSyncService(Map<String, String> config) {
        setName(getClass().getSimpleName());
        setDaemon(RunMode.isEmbedded(config));
        // 多加一个，给其他类型的调度器使用，比如集群环境下checkpoint服务线程也是个调度器
        int schedulerCount = MapUtils.getSchedulerCount(config) + 1;
        waitingSchedulers = new InternalScheduler[schedulerCount];
        redoLogRecordSyncThreshold = MapUtils.getInt(config, "redo_log_record_sync_threshold", 100);
        redoLog = new RedoLog(config, this);
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
        while (running) {
            long syncStarted = System.currentTimeMillis();
            sync();
            lastSyncedAt = syncStarted;
            if (!isPeriodic()) {
                // 如果是instant sync，只要一有redo log就接着马上同步，无需等待
                if (asyncLogQueueSize.get() > 0)
                    continue;
            } else if (asyncLogQueueSize.get() > redoLogRecordSyncThreshold) {
                // 如果是periodic sync，只要redo log达到阈值也接着马上同步，无需等待
                continue;
            }
            long now = System.currentTimeMillis();
            long sleep = syncStarted + syncIntervalMillis - now;
            if (sleep < 0)
                continue;
            awaiter.doAwait(sleep);
        }
        // 结束前最后sync一次
        sync();
        // 放在最后，让线程退出后再关闭
        redoLog.close();
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

    public void asyncWakeUp() {
        asyncLogQueueSize.getAndIncrement();
        wakeUp();
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
    }

    public void asyncWrite(AOTransaction t, RedoLogRecord r, long logId) {
        asyncWrite(new PendingTransaction(t, r, logId));
    }

    protected void asyncWrite(PendingTransaction pt) {
        InternalScheduler scheduler = pt.getScheduler();
        scheduler.addPendingTransaction(pt);
        waitingSchedulers[scheduler.getId()] = scheduler;
        asyncLogQueueSize.getAndIncrement();
        wakeUp();
    }

    public void syncWrite(AOTransaction t, RedoLogRecord r, long logId) {
        CountDownLatch latch = new CountDownLatch(1);
        addRedoLogRecord(t, r, logId, latch);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        }
    }

    public void addRedoLogRecord(AOTransaction t, RedoLogRecord r, long logId) {
        addRedoLogRecord(t, r, logId, null);
    }

    private void addRedoLogRecord(AOTransaction t, RedoLogRecord r, long logId, CountDownLatch latch) {
        PendingTransaction pt = new PendingTransaction(t, r, logId);
        pt.setCompleted(true);
        pt.setLatch(latch);
        asyncWrite(pt);
    }

    public static LogSyncService create(Map<String, String> config) {
        LogSyncService logSyncService;
        String logSyncType = config.get("log_sync_type");
        if (logSyncType == null || LOG_SYNC_TYPE_PERIODIC.equalsIgnoreCase(logSyncType))
            logSyncService = new PeriodicLogSyncService(config);
        else if (LOG_SYNC_TYPE_INSTANT.equalsIgnoreCase(logSyncType))
            logSyncService = new InstantLogSyncService(config);
        else if (LOG_SYNC_TYPE_NO_SYNC.equalsIgnoreCase(logSyncType))
            logSyncService = new NoLogSyncService(config);
        else
            throw new IllegalArgumentException("Unknow log_sync_type: " + logSyncType);
        return logSyncService;
    }
}
