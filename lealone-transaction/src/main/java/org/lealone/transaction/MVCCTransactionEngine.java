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
package org.lealone.transaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.util.DataUtils;
import org.lealone.db.Constants;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.StringDataType;
import org.lealone.storage.type.WriteBuffer;
import org.lealone.transaction.MVCCTransaction.LogRecord;
import org.lealone.transaction.log.LogMap;
import org.lealone.transaction.log.LogStorage;
import org.lealone.transaction.log.RedoLogKeyType;
import org.lealone.transaction.log.RedoLogValue;
import org.lealone.transaction.log.RedoLogValueType;
import org.lealone.transaction.log.WriteBufferPool;

public class MVCCTransactionEngine extends TransactionEngineBase {

    private static final int DEFAULT_MAP_CACHE_SIZE = 32 * 1024 * 1024; // 32M
    private static final int DEFAULT_MAP_SAVE_PERIOD = 1 * 60 * 60 * 1000; // 1小时

    private int mapCacheSize = DEFAULT_MAP_CACHE_SIZE;
    private int mapSavePeriod = DEFAULT_MAP_SAVE_PERIOD;
    private StorageMapSaveService storageMapSaveService;

    // key: mapName
    private final ConcurrentHashMap<String, StorageMap<Object, VersionedValue>> maps = new ConcurrentHashMap<>();
    // key: mapName, value: memory size
    private final ConcurrentHashMap<String, Integer> estimatedMemory = new ConcurrentHashMap<>();

    private final AtomicLong lastTransactionId = new AtomicLong();
    // key: mapName, value: map key/value ByteBuffer list
    private final HashMap<String, ArrayList<ByteBuffer>> pendingRedoLog = new HashMap<>();

    // key: transactionId
    private LogMap<Long, RedoLogValue> redoLog;
    LogStorage logStorage;

    private boolean init;
    private boolean isClusterMode;
    String hostAndPort;

    // key: transactionId
    final ConcurrentSkipListMap<Long, MVCCTransaction> currentTransactions = new ConcurrentSkipListMap<>();

    public MVCCTransactionEngine() {
        super(Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
    }

    StorageMap<Object, VersionedValue> getMap(String mapName) {
        return maps.get(mapName);
    }

    void addMap(StorageMap<Object, VersionedValue> map) {
        estimatedMemory.put(map.getName(), 0);
        maps.put(map.getName(), map);
    }

    void removeMap(String mapName) {
        estimatedMemory.remove(mapName);
        maps.remove(mapName);
    }

    private class StorageMapSaveService extends Thread {
        private final int sleep;
        private boolean running = true;
        private volatile long lastSavedAt = System.currentTimeMillis();

        StorageMapSaveService(int sleep) {
            super("StorageMapSaveService");
            this.sleep = 1000;
            setDaemon(true);
        }

        void close() {
            running = false;
        }

        @Override
        public void run() {
            Long checkpoint = null;
            while (running) {
                try {
                    sleep(sleep);
                } catch (InterruptedException e) {
                    continue;
                }

                long now = System.currentTimeMillis();

                if (redoLog.getLastSyncKey() != null)
                    checkpoint = redoLog.getLastSyncKey();

                boolean writeCheckpoint = false;
                for (Entry<String, Integer> e : estimatedMemory.entrySet()) {
                    if (e.getValue() > mapCacheSize || lastSavedAt + mapCacheSize > now) {
                        maps.get(e.getKey()).save();
                        writeCheckpoint = true;
                    }
                }
                if (lastSavedAt + mapCacheSize > now)
                    lastSavedAt = now;

                if (writeCheckpoint && checkpoint != null) {
                    redoLog.put(checkpoint, new RedoLogValue(checkpoint));
                    logStorage.logSyncService.maybeWaitForSync(redoLog, redoLog.getLastSyncKey());
                }
            }
        }
    }

    @Override
    public synchronized void init(Map<String, String> config) {
        if (init)
            return;
        init = true;
        isClusterMode = Boolean.parseBoolean(config.get("is_cluster_mode"));
        hostAndPort = config.get("host_and_port");

        String v = config.get("map_cache_size_in_mb");
        if (v != null)
            mapCacheSize = Integer.parseInt(v) * 1024 * 1024;

        v = config.get("map_save_period");
        if (v != null)
            mapSavePeriod = Integer.parseInt(v);

        int sleep = 1 * 60 * 1000;// 1分钟
        v = config.get("map_save_service_sleep_interval");
        if (v != null)
            sleep = Integer.parseInt(v);

        if (mapSavePeriod < sleep)
            sleep = mapSavePeriod;

        logStorage = new LogStorage(config);

        // 不使用ObjectDataType，因为ObjectDataType需要自动侦测，会有一些开销
        redoLog = logStorage.openLogMap("redoLog", new RedoLogKeyType(), new RedoLogValueType());
        initPendingRedoLog();

        Long key = redoLog.lastKey();
        if (key != null)
            lastTransactionId.set(key);

        if (isClusterMode)
            TransactionValidator.getInstance().start();

        storageMapSaveService = new StorageMapSaveService(sleep);
        storageMapSaveService.start();
    }

    private void initPendingRedoLog() {
        Long checkpoint = null;
        for (Entry<Long, RedoLogValue> e : redoLog.entrySet()) {
            if (e.getValue().checkpoint != null)
                checkpoint = e.getValue().checkpoint;
        }

        StorageMapCursor<Long, RedoLogValue> cursor = redoLog.cursor(checkpoint);
        while (cursor.hasNext()) {
            cursor.next();
            RedoLogValue v = cursor.getValue();
            ByteBuffer buff = v.values;
            while (buff.hasRemaining()) {
                String mapName = StringDataType.INSTANCE.read(buff);

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

    @SuppressWarnings("unchecked")
    <K> void redo(StorageMap<K, VersionedValue> map) {
        ArrayList<ByteBuffer> logs = pendingRedoLog.remove(map.getName());
        if (logs != null) {
            K key;
            Object value;
            DataType kt = map.getKeyType();
            DataType vt = ((VersionedValueType) map.getValueType()).valueType;
            for (ByteBuffer log : logs) {
                key = (K) kt.read(log);
                value = vt.read(log);
                if (value == null)
                    map.remove(key);
                else {
                    map.put(key, new VersionedValue(value));
                }
            }
        }
    }

    @Override
    public MVCCTransaction beginTransaction(boolean autoCommit) {
        if (!init) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE, "Not initialized");
        }
        long tid = nextTransactionId(autoCommit);
        MVCCTransaction t = new MVCCTransaction(this, tid, MVCCTransaction.STATUS_OPEN, 0);
        t.setAutoCommit(autoCommit);
        currentTransactions.put(tid, t);
        return t;
    }

    @Override
    public void close() {
        logStorage.close();
        if (isClusterMode)
            TransactionValidator.getInstance().close();
        if (storageMapSaveService != null) {
            storageMapSaveService.close();
            storageMapSaveService.interrupt();
        }
    }

    private long nextTransactionId(boolean autoCommit) {
        // 分布式事务使用奇数的事务ID
        if (!autoCommit && isClusterMode) {
            return nextOddTransactionId();
        }

        return nextEvenTransactionId();
    }

    long nextOddTransactionId() {
        long oldLast;
        long last;
        int delta;
        do {
            oldLast = lastTransactionId.get();
            last = oldLast;
            if (last % 2 == 0)
                delta = 1;
            else
                delta = 2;

            last += delta;
        } while (!lastTransactionId.compareAndSet(oldLast, last));
        return last;
    }

    private long nextEvenTransactionId() {
        long oldLast;
        long last;
        int delta;
        do {
            oldLast = lastTransactionId.get();
            last = oldLast;
            if (last % 2 == 0)
                delta = 2;
            else
                delta = 1;

            last += delta;
        } while (!lastTransactionId.compareAndSet(oldLast, last));
        return last;
    }

    void commit(MVCCTransaction t, RedoLogValue v) {
        // 先写redoLog
        redoLog.put(t.transactionId, v);
        logStorage.logSyncService.maybeWaitForSync(redoLog, t.transactionId);

        // 分布式事务推迟删除undoLog
        if (t.transactionId % 2 == 0) {
            removeUndoLog(t.transactionId);
        }

        endTransaction(t);
    }

    void commitAfterValidate(long tid) {
        removeUndoLog(tid);
    }

    private void removeUndoLog(long tid) {
        LinkedList<LogRecord> logRecords = currentTransactions.get(tid).logRecords;
        StorageMap<Object, VersionedValue> map;
        for (LogRecord r : logRecords) {
            map = getMap(r.mapName);
            if (map == null) {
                // map was later removed
            } else {
                VersionedValue value = map.get(r.key);
                if (value == null) {
                    // nothing to do
                } else if (value.value == null) {
                    // remove the value
                    map.remove(r.key);
                } else {
                    map.put(r.key, new VersionedValue(value.value));
                }
            }
        }
        currentTransactions.remove(tid);
    }

    RedoLogValue getRedoLog(MVCCTransaction t) {
        WriteBuffer writeBuffer = WriteBufferPool.poll();

        String mapName;
        VersionedValue value;
        StorageMap<?, ?> map;
        int lastPosition = 0, keyValueStart, memory;

        for (LogRecord r : t.logRecords) {
            mapName = r.mapName;
            value = r.newValue;
            map = maps.get(mapName);

            StringDataType.INSTANCE.write(writeBuffer, mapName);
            keyValueStart = writeBuffer.position();
            writeBuffer.putInt(0);

            map.getKeyType().write(writeBuffer, r.key);
            ((VersionedValueType) map.getValueType()).valueType.write(writeBuffer, value.value);

            writeBuffer.putInt(keyValueStart, writeBuffer.position() - keyValueStart - 4);
            memory = estimatedMemory.get(mapName);
            memory += writeBuffer.position() - lastPosition;
            lastPosition = writeBuffer.position();
            estimatedMemory.put(mapName, memory);
        }

        ByteBuffer buffer = writeBuffer.getBuffer();
        buffer.flip();
        ByteBuffer values = ByteBuffer.allocateDirect(buffer.limit());
        values.put(buffer);
        values.flip();

        WriteBufferPool.offer(writeBuffer);
        return new RedoLogValue(values);
    }

    synchronized void endTransaction(MVCCTransaction t) {
        t.setStatus(MVCCTransaction.STATUS_CLOSED);
        currentTransactions.remove(t.transactionId);
    }

    boolean validateTransaction(long tid, MVCCTransaction currentTransaction) {
        return TransactionStatusTable.validateTransaction(hostAndPort, tid, currentTransaction);
    }

    @Override
    public boolean validateTransaction(String localTransactionName) {
        return TransactionStatusTable.validateTransaction(localTransactionName);
    }

    @Override
    public boolean supportsMVCC() {
        return true;
    }
}
