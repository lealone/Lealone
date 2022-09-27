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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MapUtils;
import org.lealone.db.RunMode;
import org.lealone.transaction.RedoLogSyncListener;
import org.lealone.transaction.aote.AOTransaction;

public abstract class LogSyncService extends Thread {

    public static final String LOG_SYNC_TYPE_PERIODIC = "periodic";
    public static final String LOG_SYNC_TYPE_INSTANT = "instant";
    public static final String LOG_SYNC_TYPE_NO_SYNC = "no_sync";

    private final int waitingQueueSize;
    private final AtomicReferenceArray<RedoLogSyncListener> waitingListeners;
    private final AtomicBoolean hasWaitingListeners = new AtomicBoolean(false);
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

        waitingQueueSize = MapUtils.getInt(config, "redo_log_sync_Listener_size", 100);
        waitingListeners = new AtomicReferenceArray<>(waitingQueueSize);
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
        redoLog.save();
        notifyComplete();
    }

    private void notifyComplete() {
        if (hasWaitingListeners.compareAndSet(true, false)) {
            for (int i = 0; i < waitingQueueSize; i++) {
                RedoLogSyncListener listener = waitingListeners.get(i);
                if (listener != null) {
                    listener.wakeUpListener();
                    waitingListeners.compareAndSet(i, listener, null);
                }
            }
        }
    }

    private void wakeUp() {
        if (waiting)
            haveWork.release();
    }

    public boolean isInstantSync() {
        return false;
    }

    public boolean needSync() {
        return true;
    }

    public void asyncCommit(RedoLogRecord r, AOTransaction t, RedoLogSyncListener listener) {
        // 可能为null
        if (r == null)
            return;
        if (listener == null) {
            Object obj = Thread.currentThread();
            if (obj instanceof RedoLogSyncListener) {
                listener = (RedoLogSyncListener) obj;
            }
        }
        if (listener != null) {
            int id = listener.getListenerId();
            if (id >= 0) {
                waitingListeners.set(id, listener);
                hasWaitingListeners.set(true);
                listener.addWaitingTransaction(t);
            }
        }
        redoLog.addRedoLogRecord(r);
        wakeUp();
    }

    public void close() {
        running = false;
        wakeUp();
    }

    public void addRedoLogRecord(RedoLogRecord r) {
        // 可能为null
        if (r == null)
            return;
        redoLog.addRedoLogRecord(r);
        wakeUp();
    }

    public abstract void addAndMaybeWaitForSync(RedoLogRecord r);

    protected void addAndWaitForSync(RedoLogRecord r) {
        // 可能为null
        if (r == null)
            return;
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

    public void checkpoint(long checkpointId) {
        RedoLogRecord r = RedoLogRecord.createCheckpoint(checkpointId);
        addAndMaybeWaitForSync(r);
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
