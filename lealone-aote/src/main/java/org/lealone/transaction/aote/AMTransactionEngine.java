/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.DateTimeUtils;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.RunMode;
import org.lealone.db.SysProperties;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageEventListener;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngineBase;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.log.LogSyncService;
import org.lealone.transaction.aote.log.RedoLogRecord;
import org.lealone.transaction.aote.tvalue.TransactionalValue;

//async multi-version transaction engine
public class AMTransactionEngine extends TransactionEngineBase implements StorageEventListener {

    private static final Logger logger = LoggerFactory.getLogger(AMTransactionEngine.class);

    private static final String NAME = "AMTE";

    private static final class MapInfo {
        final StorageMap<Object, TransactionalValue> map;
        final AtomicInteger estimatedMemory = new AtomicInteger(0);

        MapInfo(StorageMap<Object, TransactionalValue> map) {
            this.map = map;
        }
    }

    // key: mapName
    private final ConcurrentHashMap<String, MapInfo> maps = new ConcurrentHashMap<>();
    // key: transactionId
    private final ConcurrentSkipListMap<Long, AMTransaction> currentTransactions = new ConcurrentSkipListMap<>();
    private final AtomicLong lastTransactionId = new AtomicLong();

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

    AMTransaction getTransaction(long tid) {
        return currentTransactions.get(tid);
    }

    Collection<AMTransaction> getCurrentTransactions() {
        return currentTransactions.values();
    }

    void addStorageMap(StorageMap<Object, TransactionalValue> map) {
        // 注意，不要敲成contains，是containsKey
        if (!maps.containsKey(map.getName())) {
            maps.put(map.getName(), new MapInfo(map));
            map.getStorage().registerEventListener(this);
        }
    }

    void removeStorageMap(String mapName) {
        maps.remove(mapName);
        RedoLogRecord r = RedoLogRecord.createDroppedMapRedoLogRecord(mapName);
        logSyncService.addAndMaybeWaitForSync(r);
    }

    ///////////////////// 以下方法在UndoLogRecord中有用途 /////////////////////

    public StorageMap<Object, TransactionalValue> getStorageMap(String mapName) {
        MapInfo mapInfo = maps.get(mapName);
        return mapInfo != null ? mapInfo.map : null;
    }

    public void incrementEstimatedMemory(String mapName, int memory) {
        MapInfo mapInfo = maps.get(mapName);
        if (mapInfo != null)
            mapInfo.estimatedMemory.addAndGet(memory);
    }

    // 看看是否有REPEATABLE_READ和SERIALIZABLE隔离级别的事务，并且事务id小于给定值tid的
    public boolean containsRepeatableReadTransactions(long lessThanVersion) {
        for (AMTransaction t : currentTransactions.headMap(lessThanVersion).values()) {
            if (t.getIsolationLevel() >= Transaction.IL_REPEATABLE_READ)
                return true;
        }
        return false;
    }

    ///////////////////// 实现TransactionEngine接口 /////////////////////

    @Override
    public synchronized void init(Map<String, String> config) {
        if (logSyncService != null)
            return;
        checkpointService = new CheckpointService(config);
        logSyncService = LogSyncService.create(config);

        long lastTransactionId = logSyncService.getRedoLog().init();
        this.lastTransactionId.set(lastTransactionId);

        // 调用完initPendingRedoLog后再启动logSyncService
        logSyncService.start();
        checkpointService.start();

        ShutdownHookUtils.addShutdownHook(this, () -> {
            close();
        });
    }

    @Override
    public synchronized void close() {
        if (logSyncService == null)
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
    public AMTransaction beginTransaction(boolean autoCommit, RunMode runMode) {
        if (logSyncService == null) {
            // 直接抛异常对上层很不友好，还不如用默认配置初始化
            init(getDefaultConfig());
        }
        long tid = getTransactionId(runMode == RunMode.SHARDING);
        AMTransaction t = createTransaction(tid, runMode);
        t.setAutoCommit(autoCommit);
        t.setRunMode(runMode);
        currentTransactions.put(tid, t);
        return t;
    }

    private static Map<String, String> getDefaultConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("base_dir", SysProperties.getBaseDir());
        config.put("redo_log_dir", "redo_log");
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        return config;
    }

    private long getTransactionId(boolean isShardingMode) {
        // 分布式事务使用奇数的事务ID
        if (isShardingMode)
            return nextOddTransactionId();
        else
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

    protected AMTransaction createTransaction(long tid, RunMode runMode) {
        return new AMTransaction(this, tid);
    }

    @Override
    public boolean validateTransaction(String globalTransactionName) {
        return false;
    }

    @Override
    public boolean supportsMVCC() {
        return true;
    }

    @Override
    public TransactionMap<?, ?> getTransactionMap(String mapName, Transaction transaction) {
        MapInfo mapInfo = maps.get(mapName);
        if (mapInfo == null)
            return null;
        else
            return new AMTransactionMap<>((AMTransaction) transaction, mapInfo.map);
        // return getTransactionMap(transaction, mapInfo.map);
    }

    protected TransactionMap<?, ?> getTransactionMap(Transaction transaction,
            StorageMap<Object, TransactionalValue> map) {
        return new AMTransactionMap<>((AMTransaction) transaction, map);
    }

    @Override
    public void checkpoint() {
        checkpointService.checkpoint();
    }

    ///////////////////// 实现StorageEventListener接口 /////////////////////

    @Override
    public void beforeClose(Storage storage) {
        // 事务引擎已经关闭了，此时忽略存储引擎的事件响应
        if (logSyncService == null)
            return;
        checkpoint();
        for (String mapName : storage.getMapNames()) {
            maps.remove(mapName);
        }
    }

    private class CheckpointService extends Thread {

        private static final int DEFAULT_COMMITTED_DATA_CACHE_SIZE = 32 * 1024 * 1024; // 32M
        private static final int DEFAULT_CHECKPOINT_PERIOD = 1 * 60 * 60 * 1000; // 1小时
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
        private synchronized void checkpoint(boolean force) {
            long now = System.currentTimeMillis();
            boolean executeCheckpoint = force || isClosed || (lastSavedAt + checkpointPeriod < now);

            // 如果上面的条件都不满足，那么再看看已经提交的数据占用的预估总内存大小是否大于阈值
            if (!executeCheckpoint) {
                long totalEstimatedMemory = 0;
                for (MapInfo mapInfo : maps.values()) {
                    totalEstimatedMemory += mapInfo.estimatedMemory.get();
                }
                executeCheckpoint = totalEstimatedMemory > committedDataCacheSize;
            }
            if (executeCheckpoint) {
                for (MapInfo mapInfo : maps.values()) {
                    StorageMap<?, ?> map = mapInfo.map;
                    if (map.isClosed())
                        continue;

                    // 在这里有可能把已提交和未提交事务的数据都保存了，
                    // 不过不要紧，如果在生成检查点之后系统崩溃了导致未提交事务不能正常完成，还有读时撤销机制保证数据完整性，
                    // 因为在保存未提交数据时，也同时保存了原来的数据，如果在读到未提交数据时发现了异常，就会进行撤销，
                    // 读时撤销机制在TransactionalValue类中实现。
                    AtomicInteger counter = mapInfo.estimatedMemory;
                    if (force || counter != null && counter.getAndSet(0) > 0) {
                        map.save();
                    }
                }
                lastSavedAt = now;
                logSyncService.checkpoint(nextEvenTransactionId());
            }
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
