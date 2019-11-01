/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.transaction.amte.log;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.lealone.common.concurrent.WaitQueue;
import org.lealone.transaction.amte.AMTransaction;

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

    public void prepareCommit(AMTransaction t) {
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

            try {
                haveWork.tryAcquire(sleep, TimeUnit.MILLISECONDS);
                haveWork.drainPermits();
            } catch (InterruptedException e) {
                throw new AssertionError();
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
        commitTransactions();
    }

    private void commitTransactions() {
        if (transactions.isEmpty())
            return;
        ArrayList<AMTransaction> oldTransactions = new ArrayList<>(transactions.size());
        transactions.drainTo(oldTransactions);
        for (AMTransaction t : oldTransactions) {
            if (t.getSession() != null)
                t.getSession().commit(null);
            else
                t.commit();
        }
    }

    public void addRedoLogRecord(RedoLogRecord r) {
        redoLog.addRedoLogRecord(r);
        // 对于需要立即做同步的场景，及时唤醒日志同步线程
        if (isInstantSync())
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
