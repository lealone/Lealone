/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
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
import org.lealone.db.link.LinkableList;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageEventListener;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngineBase;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.TransactionalValue.OldValue;
import org.lealone.transaction.aote.log.LogSyncService;
import org.lealone.transaction.aote.log.RedoLogRecord;
import org.lealone.transaction.aote.log.RedoLogRecord.PendingCheckpoint;

//Async adaptive Optimization Transaction Engine
public class AOTransactionEngine extends TransactionEngineBase implements StorageEventListener {

    private static final Logger logger = LoggerFactory.getLogger(AOTransactionEngine.class);

    private static final String NAME = "AOTE";

    private final CopyOnWriteArrayList<GcTask> gcTasks = new CopyOnWriteArrayList<>();

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
    private CheckpointServiceImpl checkpointService;

    public AOTransactionEngine() {
        super(NAME);
    }

    public AOTransactionEngine(String name) {
        super(name);
    }

    public LogSyncService getLogSyncService() {
        return logSyncService;
    }

    AOTransaction removeTransaction(long tid) {
        AOTransaction t = currentTransactions.remove(tid);
        if (t != null && t.isRepeatableRead())
            rrTransactionCount.decrementAndGet();
        return t;
    }

    AOTransaction getTransaction(long tid) {
        return currentTransactions.get(tid);
    }

    void addStorageMap(StorageMap<Object, TransactionalValue> map) {
        if (maps.putIfAbsent(map.getName(), map) == null) {
            map.getStorage().registerEventListener(this);
        }
    }

    void removeStorageMap(AOTransaction transaction, String mapName) {
        if (maps.remove(mapName) != null) {
            RedoLogRecord r = RedoLogRecord.createDroppedMapRedoLogRecord(mapName);
            logSyncService.syncWrite(transaction, r, logSyncService.nextLogId());
        }
    }

    @Override
    public void addGcTask(GcTask gcTask) {
        gcTasks.add(gcTask);
    }

    @Override
    public void removeGcTask(GcTask gcTask) {
        gcTasks.remove(gcTask);
    }

    ///////////////////// 以下方法在UndoLogRecord中有用途 /////////////////////

    public StorageMap<Object, TransactionalValue> getStorageMap(String mapName) {
        return maps.get(mapName);
    }

    @Override
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

    @Override
    public boolean containsTransaction(Long tid) {
        return currentTransactions.containsKey(tid);
    }

    @Override
    public AOTransaction getTransaction(Long tid) {
        return currentTransactions.get(tid);
    }

    @Override
    public ConcurrentSkipListMap<Long, ? extends AOTransaction> currentTransactions() {
        return currentTransactions;
    }

    ///////////////////// 实现TransactionEngine接口 /////////////////////

    @Override
    public synchronized void init(Map<String, String> config) {
        if (logSyncService != null)
            return;
        super.init(config);
        checkpointService = new CheckpointServiceImpl(config);
        logSyncService = LogSyncService.create(config);

        // 先初始化redo log
        logSyncService.getRedoLog().init();
        lastTransactionId.set(0);

        logSyncService.getRedoLog().setCheckpointService(checkpointService);

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
    public void close() {
        synchronized (this) {
            if (logSyncService == null)
                return;
            // logSyncService放在最后关闭，这样还能执行一次checkpoint，下次启动时能减少redo操作的次数
            if (checkpointService.isRunning) {
                // 先close再等待
                checkpointService.close();
            } else {
                // 当成周期性任务在调度器中执行时不用等它结束，执行一次checkpoint再close
                checkpointService.executeCheckpoint();
                checkpointService.close();
            }
        }
        if (checkpointService.isRunning) {
            try {
                checkpointService.latch.await();
            } catch (Exception e) {
            }
        }
        synchronized (this) {
            try {
                logSyncService.close();
                logSyncService.join();
            } catch (Exception e) {
            }
            this.logSyncService = null;
            this.checkpointService = null;
        }
    }

    public long nextTransactionId() {
        return lastTransactionId.incrementAndGet();
    }

    @Override
    public AOTransaction beginTransaction(boolean autoCommit, RunMode runMode, int isolationLevel) {
        if (logSyncService == null) {
            // 直接抛异常对上层很不友好，还不如用默认配置初始化
            init(getDefaultConfig());
        }
        long tid = nextTransactionId();
        AOTransaction t = createTransaction(tid, autoCommit, runMode, isolationLevel);
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

    private AOTransaction createTransaction(long tid, boolean autoCommit, RunMode runMode,
            int isolationLevel) {
        return new AOTransaction(this, tid, autoCommit, runMode, isolationLevel);
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

    protected TransactionMap<?, ?> getTransactionMap(Transaction transaction,
            StorageMap<Object, TransactionalValue> map) {
        return new AOTransactionMap<>((AOTransaction) transaction, map);
    }

    @Override
    public synchronized void checkpoint() {
        checkpointService.checkpoint();
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

    void addTransactionalValue(TransactionalValue tv, TransactionalValue.OldValue ov) {
        tValues.put(tv, ov);
    }

    TransactionalValue.OldValue getOldValue(TransactionalValue tv) {
        return tValues.get(tv);
    }

    private void removeTValues() {
        for (Entry<TransactionalValue, OldValue> e : tValues.entrySet()) {
            tValues.remove(e.getKey(), e.getValue()); // 如果不是原来的就不删除
        }
    }

    @Override
    public void fullGc(int schedulerCount, int schedulerId) {
        ArrayList<String> names = new ArrayList<>(maps.keySet());
        Collections.sort(names);
        int size = names.size();
        for (int i = 0; i < size; i++) {
            int index = i % schedulerCount;
            if (index == schedulerId) {
                StorageMap<?, ?> map = maps.get(names.get(i));
                if (!map.isClosed())
                    map.fullGc(this);
            }
        }
    }

    private static final boolean DEBUG = false;
    private final HashMap<String, Long> dirtyMaps = new HashMap<>();
    private final AtomicLong dirtyMemory = new AtomicLong();
    private long lastTime;

    private static String toM(long v) {
        return v + "(" + (v >> 10) + "K)";
    }

    private void collectDirtyMemory() {
        dirtyMaps.clear();
        dirtyMemory.set(0);

        AtomicLong usedMemory = null;
        if (DEBUG)
            usedMemory = new AtomicLong();
        for (StorageMap<?, ?> map : maps.values()) {
            if (!map.isClosed()) {
                long dm = map.collectDirtyMemory(this, usedMemory);
                if (dm > 0) {
                    dirtyMemory.addAndGet(dm);
                    dirtyMaps.put(map.getName(), dm);
                }
            }
        }
        if (DEBUG) {
            if (System.currentTimeMillis() - lastTime < 3000)
                return;
            lastTime = System.currentTimeMillis();
            logger.info("Dirty maps: " + dirtyMaps);
            logger.info("DB g_used: " + toM(MemoryManager.getGlobalMemoryManager().getUsedMemory()));
            logger.info("DB c_used: " + toM(usedMemory.get()) + ", dirty: " + toM(dirtyMemory.get()));
            MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            logger.info("JVM used: " + toM(mu.getUsed()) + ", max: " + toM(mu.getMax()));
        }
    }

    private void gc() {
        gcMaps();
        gcTValues();
        executeGcTasks();
    }

    private void gcMaps() {
        for (StorageMap<?, ?> map : maps.values()) {
            if (!map.isClosed()) {
                map.gc(this);
            }
        }
    }

    private void executeGcTasks() {
        for (GcTask gcTask : gcTasks) {
            gcTask.gc(this);
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

    @Override
    public CheckpointService getCheckpointService() {
        return checkpointService;
    }

    public class CheckpointServiceImpl implements CheckpointService, MemoryManager.MemoryListener {

        private final LinkableList<PendingCheckpoint> pendingCheckpoints = new LinkableList<>();

        // 关闭CheckpointService时等待它结束
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Semaphore semaphore = new Semaphore(1);

        private final long dirtyPageCacheSize;
        private final long checkpointPeriod;
        private final long loopInterval;

        private volatile long lastSavedAt = System.currentTimeMillis();
        private volatile boolean isClosed;
        private volatile boolean isRunning;
        private volatile boolean waiting;

        private final CopyOnWriteArrayList<Runnable> forceCheckpointTasks = new CopyOnWriteArrayList<>();

        CheckpointServiceImpl(Map<String, String> config) {
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
            collectDirtyMemory();
            long now = System.currentTimeMillis();
            boolean executeCheckpoint = force || isClosed || (lastSavedAt + checkpointPeriod < now);

            // 如果上面的条件都不满足，那么再看看已经提交的数据占用的预估总内存大小是否大于阈值
            if (!executeCheckpoint) {
                executeCheckpoint = dirtyMemory.get() > dirtyPageCacheSize;
            }
            if (executeCheckpoint) {
                long logId = logSyncService.nextLogId();
                // 1.先切换redo log chunk文件
                addPendingCheckpoint(logId, false, force);
            }
        }

        private void save(long logId, boolean force) {
            long now = System.currentTimeMillis();
            try {
                for (StorageMap<?, ?> map : maps.values()) {
                    if (map.isClosed())
                        continue;
                    Long dirtyMemory = dirtyMaps.get(map.getName());
                    if (dirtyMemory != null)
                        map.save(dirtyMemory.longValue());
                    else if (force || map.hasUnsavedChanges())
                        map.save();
                }
                lastSavedAt = now;
                // 2. 最后再把checkpoint对应的redo log放到最后那个chunk文件
                addPendingCheckpoint(logId, true, force);
            } catch (Throwable t) {
                logger.error("Failed to execute checkpoint", t);
                logSyncService.getRedoLog().ignoreCheckpoint();
            }
        }

        @Override
        public void run() {
            isRunning = true;
            MemoryManager.setGlobalMemoryListener(this);
            while (!isClosed) {
                await();
                executeCheckpoint();
                try {
                    gc();
                } catch (Throwable t) {
                    logger.error("Failed to execute gc", t);
                }
            }
            // 关闭后确保再执行一次保存点
            while (!pendingCheckpoints.isEmpty()) {
                await();
                gcPendingCheckpoints();
            }
            isRunning = false;
            MemoryManager.setGlobalMemoryListener(null);
            latch.countDown();
        }

        private void await() {
            waiting = true;
            try {
                semaphore.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
                semaphore.drainPermits();
            } catch (Throwable t) {
                logger.warn("Semaphore tryAcquire exception", t);
            } finally {
                waiting = false;
            }
        }

        @Override
        public void executeCheckpoint() {
            try {
                gcPendingCheckpoints();
                PendingCheckpoint pc = pendingCheckpoints.getHead();
                if (pc != null && !pc.isSynced()) {
                    return; // 前一个检查点第一阶段没有完成就不生成第二个检查点
                }
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
        }

        @Override
        public long getLoopInterval() {
            return loopInterval;
        }

        public PendingCheckpoint getCheckpoint() {
            return pendingCheckpoints.getHead();
        }

        private void addPendingCheckpoint(long logId, boolean saved, boolean force) {
            pendingCheckpoints.add(RedoLogRecord.createPendingCheckpoint(logId, saved, force));
            logSyncService.asyncWakeUp();
        }

        private void gcPendingCheckpoints() {
            if (pendingCheckpoints.isEmpty())
                return;
            PendingCheckpoint pc = pendingCheckpoints.getHead();
            while (pc != null && pc.isSynced()) {
                if (!pc.isSaved())
                    save(pc.getCheckpointId(), pc.isForce());
                pc = pc.getNext();
                pendingCheckpoints.decrementSize();
                pendingCheckpoints.setHead(pc);
            }
            if (pendingCheckpoints.getHead() == null)
                pendingCheckpoints.setTail(null);
        }

        @Override
        public void wakeUp() {
            if (waiting || isClosed)
                semaphore.release();
        }
    }
}
