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
package org.lealone.mvstore.mvcc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.Constants;
import org.lealone.mvstore.mvcc.MVCCTransaction.LogRecord;
import org.lealone.mvstore.mvcc.log.LogMap;
import org.lealone.mvstore.mvcc.log.LogStorage;
import org.lealone.mvstore.mvcc.log.RedoLogKeyType;
import org.lealone.mvstore.mvcc.log.RedoLogValue;
import org.lealone.mvstore.mvcc.log.RedoLogValueType;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.StringDataType;
import org.lealone.storage.type.WriteBuffer;
import org.lealone.storage.type.WriteBufferPool;
import org.lealone.transaction.TransactionEngineBase;
import org.lealone.transaction.TransactionMap;

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
        private volatile boolean isClosed;
        private volatile long lastSavedAt = System.currentTimeMillis();
        private final Semaphore semaphore = new Semaphore(1);
        private final int sleep;

        StorageMapSaveService(int sleep) {
            super("StorageMapSaveService");
            this.sleep = sleep;

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    StorageMapSaveService.this.close();
                    try {
                        StorageMapSaveService.this.join();
                    } catch (InterruptedException e) {
                    }
                }
            }, StorageMapSaveService.this.getName() + "ShutdownHook");
            Runtime.getRuntime().addShutdownHook(t);
        }

        void close() {
            if (!isClosed) {
                isClosed = true;
                semaphore.release();
            }
        }

        @Override
        public void run() {
            Long checkpoint = null;
            while (true) {
                if (isClosed)
                    break;

                try {
                    semaphore.tryAcquire(sleep, TimeUnit.MILLISECONDS);
                    semaphore.drainPermits();
                } catch (InterruptedException e) {
                    throw new AssertionError();
                }

                long now = System.currentTimeMillis();

                if (redoLog.getLastSyncKey() != null)
                    checkpoint = redoLog.getLastSyncKey();

                boolean writeCheckpoint = false;
                for (Entry<String, Integer> e : estimatedMemory.entrySet()) {
                    if (isClosed || e.getValue() > mapCacheSize || lastSavedAt + mapSavePeriod > now) {
                        maps.get(e.getKey()).save();
                        writeCheckpoint = true;
                    }
                }
                if (lastSavedAt + mapSavePeriod > now)
                    lastSavedAt = now;

                if (writeCheckpoint && checkpoint != null) {
                    redoLog.put(checkpoint, new RedoLogValue(checkpoint));
                    logStorage.logSyncService.maybeWaitForSync(redoLog, redoLog.getLastSyncKey());
                }

                if (isClosed)
                    break;
            }
        }
    }

    @Override
    public synchronized void init(Map<String, String> config) {
        if (init)
            return;
        init = true;

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
                if (log.get() == 0)
                    map.remove(key);
                else {
                    value = vt.read(log);
                    map.put(key, new VersionedValue(value));
                }
            }
        }
    }

    @Override
    public MVCCTransaction beginTransaction(boolean autoCommit, boolean isShardingMode) {
        if (!init) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE, "Not initialized");
        }
        long tid = getTransactionId(autoCommit);
        MVCCTransaction t = new MVCCTransaction(this, tid);
        t.setAutoCommit(autoCommit);
        currentTransactions.put(tid, t);
        return t;
    }

    @Override
    public void close() {
        logStorage.close();
        if (storageMapSaveService != null) {
            storageMapSaveService.close();
            try {
                storageMapSaveService.join();
            } catch (InterruptedException e) {
            }
        }
    }

    private long getTransactionId(boolean autoCommit) {
        return nextEvenTransactionId();
    }

    long nextOddTransactionId() {
        return nextTransactionId(false);
    }

    private long nextEvenTransactionId() {
        return nextTransactionId(true);
    }

    private long nextTransactionId(boolean isEven) {
        long oldLast;
        long last;
        int delta;
        do {
            oldLast = lastTransactionId.get();
            last = oldLast;
            if (last % 2 == 0)
                delta = isEven ? 2 : 1;
            else
                delta = isEven ? 1 : 2;

            last += delta;
        } while (!lastTransactionId.compareAndSet(oldLast, last));
        return last;
    }

    void prepareCommit(MVCCTransaction t, RedoLogValue v) {
        if (v != null) { // 事务没有进行任何操作时不用同步日志
            // 先写redoLog
            redoLog.put(t.transactionId, v);
        } else {
            // t.getSession().commit(false, null);
            // commit(t);
        }

        logStorage.logSyncService.prepareCommit(t);
    }

    void commit(MVCCTransaction t) {
        commitFinal(t.transactionId);
        if (t.getSession() != null && t.getSession().getCallable() != null) {
            try {
                t.getSession().getCallable().call();
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    void commit(MVCCTransaction t, RedoLogValue v) {
        if (v != null) { // 事务没有进行任何操作时不用同步日志
            // 先写redoLog
            redoLog.put(t.transactionId, v);
            logStorage.logSyncService.maybeWaitForSync(redoLog, t.transactionId);
        }

        commitFinal(t.transactionId);
    }

    private void commitFinal(long tid) {
        // 避免并发提交(TransactionValidator线程和其他读写线程都有可能在检查到分布式事务有效后帮助提交最终事务)
        MVCCTransaction t = currentTransactions.remove(tid);
        if (t == null)
            return;
        LinkedList<LogRecord> logRecords = t.logRecords;
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

        t.endTransaction();
    }

    RedoLogValue getRedoLog(MVCCTransaction t) {
        if (t.logRecords.isEmpty())
            return null;
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
            if (value.value == null)
                writeBuffer.put((byte) 0);
            else {
                writeBuffer.put((byte) 1);
                ((VersionedValueType) map.getValueType()).valueType.write(writeBuffer, value.value);
            }

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

    @Override
    public boolean supportsMVCC() {
        return true;
    }

    @Override
    public void addTransactionMap(TransactionMap<?, ?> map) {
    }

    @Override
    public TransactionMap<?, ?> getTransactionMap(String name) {
        return null;
    }

    @Override
    public boolean validateTransaction(String localTransactionName) {
        return false;
    }
}
