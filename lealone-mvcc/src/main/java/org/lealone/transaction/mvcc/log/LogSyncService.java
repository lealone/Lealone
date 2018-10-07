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
package org.lealone.transaction.mvcc.log;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.lealone.common.concurrent.WaitQueue;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.value.ValueString;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.mvcc.MVCCTransaction;
import org.lealone.transaction.mvcc.TransactionalValue;
import org.lealone.transaction.mvcc.TransactionalValueType;

public abstract class LogSyncService extends Thread {

    public static final String LOG_SYNC_TYPE_PERIODIC = "periodic";
    public static final String LOG_SYNC_TYPE_BATCH = "batch";
    public static final String LOG_SYNC_TYPE_NO_SYNC = "no_sync";

    protected final Semaphore haveWork = new Semaphore(1);
    protected final WaitQueue syncComplete = new WaitQueue();

    protected final LinkedBlockingQueue<MVCCTransaction> transactions = new LinkedBlockingQueue<>();

    protected long syncIntervalMillis;
    protected volatile long lastSyncedAt = System.currentTimeMillis();
    protected boolean running = true;
    protected RedoLog redoLog;

    // key: mapName, value: map key/value ByteBuffer list
    private final HashMap<String, ArrayList<ByteBuffer>> pendingRedoLog = new HashMap<>();

    public LogSyncService() {
        setName(getClass().getSimpleName());
        setDaemon(true);
    }

    public abstract void maybeWaitForSync(RedoLogRecord r);

    public void prepareCommit(MVCCTransaction t) {
        transactions.add(t);
        haveWork.release();
    }

    public void close() {
        running = false;
        haveWork.release(1);
        // 放在最后，让线程退出后再关闭
        redoLog.close();
    }

    @Override
    public void run() {
        ShutdownHookUtils.addShutdownHook(this, () -> {
            try {
                LogSyncService.this.sync();
            } catch (Exception e) {
            }
            try {
                LogSyncService.this.close();
                LogSyncService.this.join();
            } catch (Exception e) {
            }
        });
        while (running) {
            long syncStarted = System.currentTimeMillis();
            sync();
            lastSyncedAt = syncStarted;
            syncComplete.signalAll();
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
    }

    private void sync() {
        if (redoLog != null)
            redoLog.save();
        commitTransactions();
    }

    private void commitTransactions() {
        if (transactions.isEmpty())
            return;
        ArrayList<MVCCTransaction> oldTransactions = new ArrayList<>(transactions.size());
        transactions.drainTo(oldTransactions);
        for (MVCCTransaction t : oldTransactions) {
            if (t.getSession() != null)
                t.getSession().commit(null);
            else
                t.commit();
        }
    }

    public void addRedoLogRecord(RedoLogRecord r) {
        redoLog.addRedoLogRecord(r);
    }

    public void addAndMaybeWaitForSync(RedoLogRecord r) {
        redoLog.addRedoLogRecord(r);
        maybeWaitForSync(r);
    }

    public void writeCheckpoint() {
        RedoLogRecord r = new RedoLogRecord(true);
        addRedoLogRecord(r);
        maybeWaitForSync(r);
    }

    public long initPendingRedoLog() {
        long lastTransactionId = 0;
        for (RedoLogRecord r : redoLog.getAndResetRedoLogRecords()) {
            if (r.transactionId != null && r.transactionId > lastTransactionId) {
                lastTransactionId = r.transactionId;
            }
            if (r.droppedMap != null) {
                ArrayList<ByteBuffer> logs = pendingRedoLog.get(r.droppedMap);
                if (logs != null) {
                    logs = new ArrayList<>();
                    pendingRedoLog.put(r.droppedMap, logs);
                }
            } else {
                ByteBuffer buff = r.values;
                if (buff == null)
                    continue; // TODO 消除为NULL的可能
                while (buff.hasRemaining()) {
                    String mapName = ValueString.type.read(buff);

                    ArrayList<ByteBuffer> logs = pendingRedoLog.get(mapName);
                    if (logs == null) {
                        logs = new ArrayList<>();
                        pendingRedoLog.put(mapName, logs);
                    }
                    int len = buff.getInt();
                    byte[] keyValue = new byte[len];
                    buff.get(keyValue);
                    logs.add(ByteBuffer.wrap(keyValue));
                }
            }
        }
        return lastTransactionId;
    }

    @SuppressWarnings("unchecked")
    public <K> void redo(StorageMap<K, TransactionalValue> map) {
        ArrayList<ByteBuffer> logs = pendingRedoLog.remove(map.getName());
        if (logs != null && !logs.isEmpty()) {
            K key;
            Object value;
            StorageDataType kt = map.getKeyType();
            StorageDataType vt = ((TransactionalValueType) map.getValueType()).valueType;
            for (ByteBuffer log : logs) {
                key = (K) kt.read(log);
                if (log.get() == 0)
                    map.remove(key);
                else {
                    value = vt.read(log);
                    map.put(key, TransactionalValue.createCommitted(value));
                }
            }
        }
    }

    public static LogSyncService create(Map<String, String> config) {
        LogSyncService logSyncService;
        String logSyncType = config.get("log_sync_type");
        if (logSyncType == null || LOG_SYNC_TYPE_PERIODIC.equalsIgnoreCase(logSyncType))
            logSyncService = new PeriodicLogSyncService(config);
        else if (LOG_SYNC_TYPE_BATCH.equalsIgnoreCase(logSyncType))
            logSyncService = new BatchLogSyncService(config);
        else if (LOG_SYNC_TYPE_NO_SYNC.equalsIgnoreCase(logSyncType))
            logSyncService = new NoLogSyncService();
        else
            throw new IllegalArgumentException("Unknow log_sync_type: " + logSyncType);
        logSyncService.redoLog = new RedoLog(config);
        return logSyncService;
    }
}
