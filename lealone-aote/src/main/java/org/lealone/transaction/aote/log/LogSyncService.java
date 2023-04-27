/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.db.RunMode;

public abstract class LogSyncService extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(LogSyncService.class);

    public static final String LOG_SYNC_TYPE_PERIODIC = "periodic";
    public static final String LOG_SYNC_TYPE_INSTANT = "instant";
    public static final String LOG_SYNC_TYPE_NO_SYNC = "no_sync";

    private final Semaphore haveWork = new Semaphore(1);

    // 只要达到一定的阈值就可以立即同步了
    private final int redoLogRecordSyncThreshold;
    private final RedoLog redoLog;

    private volatile boolean running = true;
    private volatile boolean waiting;

    protected volatile long lastSyncedAt = System.currentTimeMillis();
    protected long syncIntervalMillis;

    public LogSyncService(Map<String, String> config) {
        setName(getClass().getSimpleName());
        setDaemon(RunMode.isEmbedded(config));
        redoLogRecordSyncThreshold = MapUtils.getInt(config, "redo_log_record_sync_threshold", 100);
        redoLog = new RedoLog(config);
    }

    public boolean isInstantSync() {
        return false;
    }

    public boolean needSync() {
        return true;
    }

    public RedoLog getRedoLog() {
        return redoLog;
    }

    @Override
    public void run() {
        while (running) {
            long syncStarted = System.currentTimeMillis();
            sync();
            lastSyncedAt = syncStarted;
            if (isInstantSync()) {
                // 如果是instant sync，只要一有redo log就接着马上同步，无需等待
                if (redoLog.logQueueSize() > 0)
                    continue;
            } else if (redoLog.logQueueSize() > redoLogRecordSyncThreshold) {
                // 如果是periodic sync，只要redo log达到阈值也接着马上同步，无需等待
                continue;
            }
            long now = System.currentTimeMillis();
            long sleep = syncStarted + syncIntervalMillis - now;
            if (sleep < 0)
                continue;
            waiting = true;
            try {
                haveWork.tryAcquire(sleep, TimeUnit.MILLISECONDS);
                haveWork.drainPermits();
            } catch (InterruptedException e) {
                throw new AssertionError();
            } finally {
                waiting = false;
            }
        }
        // 结束前最后sync一次
        sync();
        // 放在最后，让线程退出后再关闭
        redoLog.close();
    }

    private void sync() {
        try {
            redoLog.save();
        } catch (Exception e) {
            logger.error("Failed to sync redo log", e);
        }
    }

    private void wakeUp() {
        if (waiting)
            haveWork.release();
    }

    public void close() {
        running = false;
        wakeUp();
    }

    public void asyncWrite(RedoLogRecord r) {
        redoLog.addRedoLogRecord(r);
        wakeUp();
    }

    public void syncWrite(RedoLogRecord r) {
        CountDownLatch latch = new CountDownLatch(1);
        r.setLatch(latch);
        redoLog.addRedoLogRecord(r);
        wakeUp();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        }
    }

    public void checkpoint(long checkpointId, boolean saved) {
        RedoLogRecord r = RedoLogRecord.createCheckpoint(checkpointId, saved);
        asyncWrite(r);
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
