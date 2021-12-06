/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.lealone.common.concurrent.WaitQueue;
import org.lealone.transaction.aote.AMTransaction;

public abstract class LogSyncService extends Thread {

    public static final String LOG_SYNC_TYPE_PERIODIC = "periodic";
    public static final String LOG_SYNC_TYPE_INSTANT = "instant";
    public static final String LOG_SYNC_TYPE_NO_SYNC = "no_sync";

    protected final Semaphore haveWork = new Semaphore(1);
    protected final WaitQueue syncComplete = new WaitQueue();

    // 只要达到一定的阈值就可以立即同步了
    protected final int redoLogRecordSyncThreshold;
    protected final LinkedBlockingQueue<AMTransaction> transactions = new LinkedBlockingQueue<>();

    protected long syncIntervalMillis;
    protected volatile long lastSyncedAt = System.currentTimeMillis();
    protected boolean running = true;
    protected RedoLog redoLog;

    private volatile boolean waiting;

    public LogSyncService(Map<String, String> config) {
        setName(getClass().getSimpleName());
        setDaemon(true);
        if (config.containsKey("redo_log_record_sync_threshold"))
            redoLogRecordSyncThreshold = Integer.parseInt(config.get("redo_log_record_sync_threshold"));
        else
            redoLogRecordSyncThreshold = 100;
    }

    public RedoLog getRedoLog() {
        return redoLog;
    }

    public abstract void maybeWaitForSync(RedoLogRecord r);

    public void asyncCommit(AMTransaction t) {
        transactions.add(t);
        haveWork.release();
    }

    public void close() {
        running = false;
        haveWork.release(1);
    }

    @Override
    public void run() {
        while (running) {
            long syncStarted = System.currentTimeMillis();
            sync();
            lastSyncedAt = syncStarted;
            syncComplete.signalAll();
            if (redoLog.size() > redoLogRecordSyncThreshold)
                continue;
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
        if (redoLog != null)
            redoLog.save();
        notifyComplete();
    }

    private void notifyComplete() {
        if (transactions.isEmpty())
            return;
        ArrayList<AMTransaction> oldTransactions = new ArrayList<>(transactions.size());
        transactions.drainTo(oldTransactions);
        for (AMTransaction t : oldTransactions) {
            t.asyncCommitComplete();
        }
    }

    public void addRedoLogRecord(RedoLogRecord r) {
        redoLog.addRedoLogRecord(r);
        // 对于需要立即做同步的场景，及时唤醒日志同步线程
        if (isInstantSync() || waiting)
            haveWork.release();
    }

    public void addAndMaybeWaitForSync(RedoLogRecord r) {
        redoLog.addRedoLogRecord(r);
        maybeWaitForSync(r);
    }

    public void checkpoint(long checkpointId) {
        RedoLogRecord r = RedoLogRecord.createCheckpoint(checkpointId);
        addRedoLogRecord(r);
        maybeWaitForSync(r);
    }

    public boolean isInstantSync() {
        return false;
    }

    public boolean needSync() {
        return true;
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
        logSyncService.redoLog = new RedoLog(config);
        return logSyncService;
    }
}
