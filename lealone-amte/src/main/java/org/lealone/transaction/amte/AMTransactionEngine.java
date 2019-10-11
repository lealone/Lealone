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
package org.lealone.transaction.amte;

import java.util.Collection;
import java.util.Map;
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
import org.lealone.common.util.DateTimeUtils;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageEventListener;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.TransactionEngineBase;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.amte.log.LogSyncService;
import org.lealone.transaction.amte.log.RedoLogRecord;

//async multi-version transaction engine
public class AMTransactionEngine extends TransactionEngineBase implements StorageEventListener {

    private static final Logger logger = LoggerFactory.getLogger(AMTransactionEngine.class);

    private static final String NAME = "AMTE";

    // key: mapName
    private final ConcurrentHashMap<String, StorageMap<Object, TransactionalValue>> maps = new ConcurrentHashMap<>();
    // key: mapName
    private final ConcurrentHashMap<String, TransactionMap<?, ?>> tmaps = new ConcurrentHashMap<>();
    // key: mapName, value: memory size
    private final ConcurrentHashMap<String, AtomicInteger> estimatedMemory = new ConcurrentHashMap<>();
    // key: transactionId
    private final ConcurrentSkipListMap<Long, AMTransaction> currentTransactions = new ConcurrentSkipListMap<>();

    private final AtomicLong lastTransactionId = new AtomicLong();
    private final AtomicBoolean init = new AtomicBoolean(false);

    private LogSyncService logSyncService;
    private CheckpointService checkpointService;

    public AMTransactionEngine() {
        super(NAME);
    }

    public AMTransactionEngine(String name) {
        super(name);
    }

    public LogSyncService getLogSyncService() {
        return logSyncService;
    }

    AMTransaction removeTransaction(long tid) {
        return currentTransactions.remove(tid);
    }

    boolean containsTransaction(long tid) {
        return currentTransactions.containsKey(tid);
    }

    boolean containsUncommittedTransactionLessThan(long tid) {
        return currentTransactions.lowerKey(tid) != null;
    }

    AMTransaction getTransaction(long tid) {
        return currentTransactions.get(tid);
    }

    Collection<AMTransaction> getCurrentTransactions() {
        return currentTransactions.values();
    }

    StorageMap<Object, TransactionalValue> getMap(String mapName) {
        return maps.get(mapName);
    }

    void addMap(StorageMap<Object, TransactionalValue> map) {
        estimatedMemory.put(map.getName(), new AtomicInteger(0));
        maps.put(map.getName(), map);
        map.getStorage().registerEventListener(this);
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
        checkpointService = new CheckpointService(config);
        logSyncService = LogSyncService.create(config);

        long lastTransactionId = logSyncService.initPendingRedoLog();
        this.lastTransactionId.set(lastTransactionId);

        // 调用完initPendingRedoLog后再启动logSyncService
        logSyncService.start();
        checkpointService.start();

        addShutdownHook();
    }

    private void addShutdownHook() {
        ShutdownHookUtils.addShutdownHook(this, () -> {
            close();
        });
    }

    @Override
    public AMTransaction beginTransaction(boolean autoCommit, boolean isShardingMode) {
        if (!init.get()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE, "Not initialized");
        }
        long tid = getTransactionId(autoCommit, isShardingMode);
        AMTransaction t = createTransaction(tid);
        t.setAutoCommit(autoCommit);
        currentTransactions.put(tid, t);
        return t;
    }

    protected AMTransaction createTransaction(long tid) {
        return new AMTransaction(this, tid);
    }

    @Override
    public void close() {
        if (!init.compareAndSet(true, false))
            return;
        if (logSyncService != null) {
            // logSyncService放在最后关闭，这样还能执行一次checkpoint，下次启动时能减少redo操作的次数
            try {
                checkpointService.close();
                checkpointService.join();
            } catch (Exception e) {
            }
            try {
                logSyncService.close();
                logSyncService.join();
            } catch (Exception e) {
            }
            this.logSyncService = null;
            this.checkpointService = null;
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
        checkpointService.checkpoint();
    }

    @Override
    public void beforeClose(Storage storage) {
        checkpoint();
        for (String mapName : storage.getMapNames()) {
            estimatedMemory.remove(mapName);
            maps.remove(mapName);
            tmaps.remove(mapName);
        }
    }

    private class CheckpointService extends Thread {

        private static final int DEFAULT_COMMITTED_DATA_CACHE_SIZE = 32 * 1024 * 1024; // 32M
        private static final int DEFAULT_CHECKPOINT_PERIOD = 1 * 60 * 60 * 1000; // 1小时
        private final AtomicBoolean checking = new AtomicBoolean(false);
        private final Semaphore semaphore = new Semaphore(1);
        private final int committedDataCacheSize;
        private final long checkpointPeriod;
        private final long loopInterval;

        private volatile long lastSavedAt = System.currentTimeMillis();
        private volatile boolean isClosed;

        CheckpointService(Map<String, String> config) {
            setName(getClass().getSimpleName());
            setDaemon(true);

            String v = config.get("committed_data_cache_size_in_mb");
            if (v != null)
                committedDataCacheSize = Integer.parseInt(v) * 1024 * 1024;
            else
                committedDataCacheSize = DEFAULT_COMMITTED_DATA_CACHE_SIZE;

            v = config.get("checkpoint_period");
            if (v != null)
                checkpointPeriod = Long.parseLong(v);
            else
                checkpointPeriod = DEFAULT_CHECKPOINT_PERIOD;

            // 默认1分钟
            long loopInterval = DateTimeUtils.getLoopInterval(config, "checkpoint_service_loop_interval",
                    1 * 60 * 1000);

            if (checkpointPeriod < loopInterval)
                loopInterval = checkpointPeriod;

            this.loopInterval = loopInterval;
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
            boolean executeCheckpoint = force || isClosed || (lastSavedAt + checkpointPeriod < now);

            // 如果上面的条件都不满足，那么再看看已经提交的数据占用的预估总内存大小是否大于阈值
            if (!executeCheckpoint) {
                long totalEstimatedMemory = 0;
                for (AtomicInteger counter : estimatedMemory.values()) {
                    totalEstimatedMemory += counter.get();
                }
                executeCheckpoint = totalEstimatedMemory > committedDataCacheSize;
            }

            if (executeCheckpoint) {
                for (StorageMap<Object, TransactionalValue> map : maps.values()) {
                    if (map.isClosed())
                        continue;

                    // 在这里有可能把已提交和未提交事务的数据都保存了，
                    // 不过不要紧，如果在生成检查点之后系统崩溃了导致未提交事务不能正常完成，还有读时撤销机制保证数据完整性，
                    // 因为在保存未提交数据时，也同时保存了原来的数据，如果在读到未提交数据时发现了异常，就会进行撤销，
                    // 读时撤销机制在TransactionalValue类中实现。
                    AtomicInteger counter = estimatedMemory.get(map.getName());
                    if (counter != null && counter.getAndSet(0) > 0) {
                        map.save();
                    }
                }
                lastSavedAt = now;
                logSyncService.checkpoint(nextEvenTransactionId());
            }
            checking.set(false);
        }

        @Override
        public void run() {
            while (!isClosed) {
                try {
                    semaphore.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
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
