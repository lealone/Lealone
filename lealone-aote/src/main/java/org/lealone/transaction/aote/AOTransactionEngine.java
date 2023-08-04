/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.MemoryManager;
import org.lealone.db.RunMode;
import org.lealone.db.SysProperties;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageEventListener;
import org.lealone.storage.StorageMap;
import org.lealone.storage.lob.LobStorage;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngineBase;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.TransactionalValue.OldValue;
import org.lealone.transaction.aote.log.LogSyncService;
import org.lealone.transaction.aote.log.RedoLogRecord;

public class AOTransactionEngine extends TransactionEngineBase implements StorageEventListener {

    private static final Logger logger = LoggerFactory.getLogger(AOTransactionEngine.class);

    private final CopyOnWriteArrayList<LobStorage> lobStorages = new CopyOnWriteArrayList<>();

    // key: mapName
    private final ConcurrentHashMap<String, StorageMap<Object, TransactionalValue>> maps //
            = new ConcurrentHashMap<>();

    // key: transactionId
    private final ConcurrentSkipListMap<Long, AOTransaction> currentTransactions //
            = new ConcurrentSkipListMap<>();

    private final ConcurrentHashMap<TransactionalValue, TransactionalValue.OldValue> tValues //
            = new ConcurrentHashMap<>();

    private final AtomicLong lastTransactionId = new AtomicLong();
    // repeatable read 事务数
    private final AtomicInteger rrTransactionCount = new AtomicInteger();

    private LogSyncService logSyncService;
    private CheckpointService checkpointService;

    public AOTransactionEngine() {
        super("AOTE");
    }

    LogSyncService getLogSyncService() {
        return logSyncService;
    }

    AOTransaction removeTransaction(long tid) {
        AOTransaction t = currentTransactions.remove(tid);
        if (t != null && t.isRepeatableRead())
            rrTransactionCount.decrementAndGet();
        return t;
    }

    boolean containsTransaction(long tid) {
        return currentTransactions.containsKey(tid);
    }

    Collection<AOTransaction> getCurrentTransactions() {
        return currentTransactions.values();
    }

    void addStorageMap(StorageMap<Object, TransactionalValue> map) {
        if (maps.putIfAbsent(map.getName(), map) == null) {
            map.getStorage().registerEventListener(this);
        }
    }

    void removeStorageMap(String mapName) {
        if (maps.remove(mapName) != null) {
            RedoLogRecord r = RedoLogRecord.createDroppedMapRedoLogRecord(mapName);
            logSyncService.syncWrite(r);
        }
    }

    void addTransactionalValue(TransactionalValue tv, TransactionalValue.OldValue ov) {
        tValues.put(tv, ov);
    }

    TransactionalValue.OldValue getOldValue(TransactionalValue tv) {
        return tValues.get(tv);
    }

    ///////////////////// 以下方法在UndoLogRecord中有用途 /////////////////////

    public StorageMap<Object, TransactionalValue> getStorageMap(String mapName) {
        return maps.get(mapName);
    }

    public boolean containsRepeatableReadTransactions() {
        return rrTransactionCount.get() > 0;
    }

    public long getMaxRepeatableReadTransactionId() {
        long maxTid = -1;
        for (AOTransaction t : currentTransactions.values()) {
            if (t.isRepeatableRead() && t.getTransactionId() > maxTid)
                maxTid = t.getTransactionId();
        }
        return maxTid;
    }

    ///////////////////// 实现TransactionEngine接口 /////////////////////

    @Override
    public synchronized void init(Map<String, String> config) {
        if (logSyncService != null)
            return;
        super.init(config);
        checkpointService = new CheckpointService(config);
        logSyncService = LogSyncService.create(config);

        // 先初始化redo log
        logSyncService.getRedoLog().init();
        lastTransactionId.set(0);

        // 再启动logSyncService
        logSyncService.start();

        if (RunMode.isEmbedded(config)) {
            Thread t = new Thread(checkpointService, "CheckpointService");
            t.setDaemon(true);
            t.start();
        }

        ShutdownHookUtils.addShutdownHook(this, () -> {
            close();
        });
    }

    @Override
    public synchronized void close() {
        if (logSyncService == null)
            return;
        // logSyncService放在最后关闭，这样还能执行一次checkpoint，下次启动时能减少redo操作的次数
        try {
            checkpointService.close();
            if (checkpointService.isRunning)
                checkpointService.latch.await();
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

    @Override
    public AOTransaction beginTransaction(boolean autoCommit, int isolationLevel) {
        if (logSyncService == null) {
            // 直接抛异常对上层很不友好，还不如用默认配置初始化
            init(getDefaultConfig());
        }
        long tid = nextTransactionId();
        AOTransaction t = new AOTransaction(this, tid, autoCommit, isolationLevel);
        if (t.isRepeatableRead())
            rrTransactionCount.incrementAndGet();
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

    public long nextTransactionId() {
        return lastTransactionId.incrementAndGet();
    }

    @Override
    public boolean supportsMVCC() {
        return true;
    }

    @Override
    public TransactionMap<?, ?> getTransactionMap(String mapName, Transaction transaction) {
        StorageMap<Object, TransactionalValue> map = maps.get(mapName);
        if (map == null)
            return null;
        else
            return new AOTransactionMap<>((AOTransaction) transaction, map);
    }

    @Override
    public synchronized void checkpoint() {
        checkpointService.checkpoint();
    }

    @Override
    public Runnable getRunnable() {
        return checkpointService;
    }

    @Override
    public void addLobStorage(LobStorage lobStorage) {
        lobStorages.add(lobStorage);
    }

    @Override
    public void removeLobStorage(LobStorage lobStorage) {
        lobStorages.remove(lobStorage);
    }

    ///////////////////// 实现StorageEventListener接口 /////////////////////

    @Override
    public synchronized void beforeClose(Storage storage) {
        // 事务引擎已经关闭了，此时忽略存储引擎的事件响应
        if (logSyncService == null)
            return;
        checkpoint();
        for (String mapName : storage.getMapNames()) {
            maps.remove(mapName);
        }
    }

    private void gc() {
        gcMaps();
        gcLobStorages();
        gcTValues();
    }

    private void gcMaps() {
        for (StorageMap<?, ?> map : maps.values()) {
            if (!map.isClosed())
                map.gc(currentTransactions);
        }
    }

    private void gcLobStorages() {
        for (LobStorage lobStorage : lobStorages) {
            lobStorage.gc(currentTransactions);
        }
    }

    private void gcTValues() {
        if (tValues.isEmpty())
            return;
        if (!containsRepeatableReadTransactions()) {
            removeTValues();
            return;
        }
        long minTid = Long.MAX_VALUE;
        for (AOTransaction t : currentTransactions.values()) {
            if (t.isRepeatableRead() && t.getTransactionId() < minTid)
                minTid = t.getTransactionId();
        }
        if (minTid != Long.MAX_VALUE) {
            for (Entry<TransactionalValue, OldValue> e : tValues.entrySet()) {
                OldValue oldValue = e.getValue();
                if (oldValue != null && oldValue.tid < minTid) {
                    tValues.remove(e.getKey(), oldValue); // 如果不是原来的就不删除
                    continue;
                }
                while (oldValue != null) {
                    if (oldValue.tid < minTid) {
                        oldValue.next = null;
                        break;
                    }
                    oldValue = oldValue.next;
                }
            }
        } else {
            removeTValues();
        }
    }

    private void removeTValues() {
        for (Entry<TransactionalValue, OldValue> e : tValues.entrySet()) {
            tValues.remove(e.getKey(), e.getValue()); // 如果不是原来的就不删除
        }
    }

    private class CheckpointService implements Runnable {
        // 关闭CheckpointService时等待它结束
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Semaphore semaphore = new Semaphore(1);

        private final long dirtyPageCacheSize;
        private final long checkpointPeriod;
        private final long loopInterval;

        private volatile long lastSavedAt = System.currentTimeMillis();
        private volatile boolean isClosed;
        private volatile boolean isRunning;

        private final CopyOnWriteArrayList<Runnable> forceCheckpointTasks = new CopyOnWriteArrayList<>();

        CheckpointService(Map<String, String> config) {
            // 默认32M
            dirtyPageCacheSize = MapUtils.getLongMB(config, "dirty_page_cache_size_in_mb",
                    32 * 1024 * 1024);
            // 默认1小时
            checkpointPeriod = MapUtils.getLong(config, "checkpoint_period", 1 * 60 * 60 * 1000);
            // 默认3秒钟
            long loopInterval = MapUtils.getLong(config, "checkpoint_service_loop_interval", 3 * 1000);
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
            // 异步执行checkpoint命令
            forceCheckpointTasks.add(() -> checkpoint(true));
            semaphore.release();
        }

        // 按周期自动触发
        private synchronized void checkpoint(boolean force) {
            long now = System.currentTimeMillis();
            boolean executeCheckpoint = force || isClosed || (lastSavedAt + checkpointPeriod < now);

            // 如果上面的条件都不满足，那么再看看已经提交的数据占用的预估总内存大小是否大于阈值
            if (!executeCheckpoint) {
                executeCheckpoint = MemoryManager.getGlobalMemoryManager()
                        .getDirtyMemory() > dirtyPageCacheSize;
            }
            if (executeCheckpoint) {
                // 1.先切换redo log chunk文件
                logSyncService.checkpoint(false);
                try {
                    for (StorageMap<?, ?> map : maps.values()) {
                        if (map.isClosed())
                            continue;
                        if (force || map.hasUnsavedChanges())
                            map.save();
                    }
                    lastSavedAt = now;
                    // 2. 最后再把checkpoint对应的redo log放到最后那个chunk文件
                    logSyncService.checkpoint(true);
                } catch (Throwable t) {
                    logger.error("Failed to execute checkpoint", t);
                    logSyncService.getRedoLog().ignoreCheckpoint();
                }
            }
        }

        @Override
        public void run() {
            isRunning = true;
            while (!isClosed) {
                try {
                    semaphore.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
                    semaphore.drainPermits();
                } catch (Throwable t) {
                    logger.warn("Semaphore tryAcquire exception", t);
                }

                try {
                    if (!forceCheckpointTasks.isEmpty()) {
                        ArrayList<Runnable> tasks = new ArrayList<>(forceCheckpointTasks);
                        forceCheckpointTasks.removeAll(tasks);
                        for (Runnable task : tasks)
                            task.run();
                    } else {
                        checkpoint(false);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to execute checkpoint", t);
                }

                try {
                    gc();
                } catch (Throwable t) {
                    logger.error("Failed to execute gc", t);
                }
            }
            isRunning = false;
            latch.countDown();
        }
    }
}
