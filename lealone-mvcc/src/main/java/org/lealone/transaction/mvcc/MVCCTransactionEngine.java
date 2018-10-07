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
package org.lealone.transaction.mvcc;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.TransactionEngineBase;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.mvcc.log.LogSyncService;
import org.lealone.transaction.mvcc.log.RedoLogRecord;

public class MVCCTransactionEngine extends TransactionEngineBase {

    private static final Logger logger = LoggerFactory.getLogger(MVCCTransactionEngine.class);

    private static final String NAME = "MVCC";

    // key: mapName
    private final ConcurrentHashMap<String, StorageMap<Object, TransactionalValue>> maps = new ConcurrentHashMap<>();
    // key: mapName
    private final ConcurrentHashMap<String, TransactionMap<?, ?>> tmaps = new ConcurrentHashMap<>();
    // key: mapName, value: memory size
    private final ConcurrentHashMap<String, AtomicInteger> estimatedMemory = new ConcurrentHashMap<>();
    // key: transactionId
    private final ConcurrentSkipListMap<Long, MVCCTransaction> currentTransactions = new ConcurrentSkipListMap<>();

    private final AtomicLong lastTransactionId = new AtomicLong();
    private final AtomicBoolean init = new AtomicBoolean(false);

    private LogSyncService logSyncService;
    private StorageMapSaveService storageMapSaveService;

    public MVCCTransactionEngine() {
        super(NAME);
    }

    public MVCCTransactionEngine(String name) {
        super(name);
    }

    public LogSyncService getLogSyncService() {
        return logSyncService;
    }

    MVCCTransaction removeTransaction(long tid) {
        return currentTransactions.remove(tid);
    }

    boolean containsTransaction(long tid) {
        return currentTransactions.containsKey(tid);
    }

    MVCCTransaction getTransaction(long tid) {
        return currentTransactions.get(tid);
    }

    Collection<MVCCTransaction> getCurrentTransactions() {
        return currentTransactions.values();
    }

    StorageMap<Object, TransactionalValue> getMap(String mapName) {
        return maps.get(mapName);
    }

    void addMap(StorageMap<Object, TransactionalValue> map) {
        estimatedMemory.put(map.getName(), new AtomicInteger(0));
        maps.put(map.getName(), map);
    }

    void removeMap(String mapName) {
        estimatedMemory.remove(mapName);
        maps.remove(mapName);
        RedoLogRecord r = RedoLogRecord.createDroppedMapRedoLogRecord(mapName);
        logSyncService.addAndMaybeWaitForSync(r);
    }

    void incrementEstimatedMemory(String mapName, int memory) {
        estimatedMemory.get(mapName).addAndGet(memory);
    }

    @Override
    public synchronized void init(Map<String, String> config) {
        if (!init.compareAndSet(false, true))
            return;
        storageMapSaveService = new StorageMapSaveService(config);
        logSyncService = LogSyncService.create(config);

        long lastTransactionId = logSyncService.initPendingRedoLog();
        this.lastTransactionId.set(lastTransactionId);

        // 调用完initPendingRedoLog后再启动logSyncService
        logSyncService.start();
        storageMapSaveService.start();
    }

    @Override
    public MVCCTransaction beginTransaction(boolean autoCommit, boolean isShardingMode) {
        if (!init.get()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE, "Not initialized");
        }
        long tid = getTransactionId(autoCommit, isShardingMode);
        MVCCTransaction t = createTransaction(tid);
        t.setAutoCommit(autoCommit);
        currentTransactions.put(tid, t);
        return t;
    }

    protected MVCCTransaction createTransaction(long tid) {
        return new MVCCTransaction(this, tid);
    }

    @Override
    public void close() {
        if (logSyncService != null) {
            try {
                logSyncService.close();
                logSyncService.join();
            } catch (Exception e) {
            }
            try {
                storageMapSaveService.close();
                storageMapSaveService.join();
            } catch (Exception e) {
            }
            logSyncService = null;
            storageMapSaveService = null;
        }
    }

    @Override
    public boolean validateTransaction(String localTransactionName) {
        return false;
    }

    @Override
    public boolean supportsMVCC() {
        return true;
    }

    @Override
    public void addTransactionMap(TransactionMap<?, ?> map) {
        tmaps.put(map.getName(), map);
    }

    @Override
    public TransactionMap<?, ?> getTransactionMap(String name) {
        return tmaps.get(name);
    }

    @Override
    public void removeTransactionMap(String name) {
        removeMap(name);
        tmaps.remove(name);
    }

    private long getTransactionId(boolean autoCommit, boolean isShardingMode) {
        // 分布式事务使用奇数的事务ID
        if (!autoCommit && isShardingMode) {
            return nextOddTransactionId();
        }
        return nextEvenTransactionId();
    }

    public long nextOddTransactionId() {
        return nextTransactionId(false);
    }

    public long nextEvenTransactionId() {
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

    @Override
    public void checkpoint() {
        storageMapSaveService.checkpoint();
    }

    private class StorageMapSaveService extends Thread {

        private static final int DEFAULT_MAP_CACHE_SIZE = 32 * 1024 * 1024; // 32M
        private static final int DEFAULT_MAP_SAVE_PERIOD = 1 * 60 * 60 * 1000; // 1小时

        private final AtomicBoolean checking = new AtomicBoolean(false);
        private final Semaphore semaphore = new Semaphore(1);
        private final int mapCacheSize;
        private final int mapSavePeriod;
        private final int sleep;

        private volatile long lastSavedAt = System.currentTimeMillis();
        private volatile boolean isClosed;

        StorageMapSaveService(Map<String, String> config) {
            super("StorageMapSaveService");
            setDaemon(true);

            String v = config.get("map_cache_size_in_mb");
            if (v != null)
                mapCacheSize = Integer.parseInt(v) * 1024 * 1024;
            else
                mapCacheSize = DEFAULT_MAP_CACHE_SIZE;

            v = config.get("map_save_period");
            if (v != null)
                mapSavePeriod = Integer.parseInt(v);
            else
                mapSavePeriod = DEFAULT_MAP_SAVE_PERIOD;

            int sleep = 1 * 60 * 1000;// 1分钟
            v = config.get("map_save_service_sleep_interval");
            if (v != null)
                sleep = Integer.parseInt(v);

            if (mapSavePeriod < sleep)
                sleep = mapSavePeriod;

            this.sleep = sleep;
            ShutdownHookUtils.addShutdownHook(this, () -> {
                StorageMapSaveService.this.close();
                try {
                    StorageMapSaveService.this.join();
                } catch (InterruptedException e) {
                }
            });
        }

        void close() {
            if (!isClosed) {
                isClosed = true;
                semaphore.release();
            }
        }

        // 例如通过执行CHECKPOINT语句触发
        void checkpoint() {
            if (isClosed)
                return;
            checkpoint(true);
        }

        // 按周期自动触发
        private void checkpoint(boolean force) {
            if (!checking.compareAndSet(false, true))
                return;
            long now = System.currentTimeMillis();
            boolean executeCheckpoint = false;
            boolean forceSave = force || isClosed || (lastSavedAt + mapSavePeriod < now);
            for (Entry<String, AtomicInteger> e : estimatedMemory.entrySet()) {
                if (forceSave || executeCheckpoint || e.getValue().get() > mapCacheSize) {
                    maps.get(e.getKey()).save();
                    e.getValue().set(0);
                    executeCheckpoint = true;
                }
            }
            if (executeCheckpoint) {
                lastSavedAt = now;
                logSyncService.checkpoint(nextEvenTransactionId());
            }
            checking.set(false);
        }

        @Override
        public void run() {
            while (!isClosed) {
                try {
                    semaphore.tryAcquire(sleep, TimeUnit.MILLISECONDS);
                    semaphore.drainPermits();
                } catch (InterruptedException e) {
                    throw new AssertionError();
                }
                try {
                    checkpoint(false);
                } catch (Exception e) {
                    logger.error("Failed to execute checkpoint", e);
                }
            }
        }
    }
}
