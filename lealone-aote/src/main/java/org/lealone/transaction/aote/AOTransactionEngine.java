/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.RunMode;
import org.lealone.db.SysProperties;
import org.lealone.db.async.AsyncPeriodicTask;
import org.lealone.db.scheduler.EmbeddedScheduler;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.db.scheduler.SchedulerFactory;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageEventListener;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngineBase;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.TransactionalValue.OldValue;
import org.lealone.transaction.aote.log.LogSyncService;
import org.lealone.transaction.aote.log.RedoLogRecord;

//Async adaptive Optimization Transaction Engine
public class AOTransactionEngine extends TransactionEngineBase implements StorageEventListener {

    private static final String NAME = "AOTE";

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

    LogSyncService logSyncService;
    private CheckpointService masterCheckpointService;
    CheckpointService[] checkpointServices;

    SchedulerFactory schedulerFactory;

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
            Scheduler scheduler = schedulerFactory.getScheduler();
            checkpointServices[scheduler.getId()].addMap(map);
        }
    }

    void removeStorageMap(AOTransaction transaction, String mapName) {
        if (maps.remove(mapName) != null) {
            RedoLogRecord r = RedoLogRecord.createDroppedMapRedoLogRecord(mapName);
            logSyncService.syncWrite(transaction, r, logSyncService.nextLogId());
            for (int i = 0; i < checkpointServices.length; i++) {
                checkpointServices[i].removeMap(mapName);
            }
        }
    }

    @Override
    public void addGcTask(GcTask gcTask) {
        Scheduler scheduler = schedulerFactory.getScheduler();
        checkpointServices[scheduler.getId()].addGcTask(gcTask);
    }

    @Override
    public void removeGcTask(GcTask gcTask) {
        for (int i = 0; i < checkpointServices.length; i++) {
            checkpointServices[i].removeGcTask(gcTask);
        }
    }

    @Override
    public void fullGc(int schedulerId) {
        checkpointServices[schedulerId].fullGc();
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
        initCheckpointService();
        initLogSyncService();

        ShutdownHookUtils.addShutdownHook(this, () -> {
            close();
        });
    }

    @Override
    public void close() {
        CountDownLatch latch = null;
        synchronized (this) {
            if (logSyncService == null)
                return;
            if (masterCheckpointService.isRunning()) {
                latch = new CountDownLatch(1);
                masterCheckpointService.executeCheckpointOnClose(latch);
            }
        }
        if (latch != null) {
            try {
                latch.wait();
            } catch (Exception e) {
            }
        }
        synchronized (this) {
            try {
                logSyncService.close();
                logSyncService.join();
            } catch (Exception e) {
            }
            logSyncService = null;
            masterCheckpointService = null;
            checkpointServices = null;
        }
        super.close();
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
        masterCheckpointService.executeCheckpointAsync();
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

    void gcTValues() {
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
    public Runnable getFsyncService() {
        return logSyncService;
    }

    private void initCheckpointService() {
        SchedulerFactory sf = SchedulerFactory.getDefaultSchedulerFactory();
        if (sf == null) {
            sf = SchedulerFactory.initDefaultSchedulerFactory(EmbeddedScheduler.class.getName(), config);
        }
        schedulerFactory = sf;
        Scheduler[] schedulers = schedulerFactory.getSchedulers();
        int schedulerCount = schedulers.length;

        checkpointServices = new CheckpointService[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            checkpointServices[i] = new CheckpointService(this, config, i);
        }
        for (int i = 0; i < schedulerCount; i++) {
            CheckpointService cs = checkpointServices[i];
            AsyncPeriodicTask task = new AsyncPeriodicTask(1000, cs.getLoopInterval(), () -> cs.run());
            cs.setAsyncPeriodicTask(task);
            schedulers[i].addPeriodicTask(task);
        }
        masterCheckpointService = checkpointServices[0];
    }

    private void initLogSyncService() {
        // 初始化redo log
        logSyncService = LogSyncService.create(config);
        logSyncService.getRedoLog().init();
        lastTransactionId.set(0);

        logSyncService.getRedoLog().setCheckpointService(masterCheckpointService);

        // 嵌入式场景需要启动logSyncService
        if (RunMode.isEmbedded(config)) {
            logSyncService.start();
        }
    }
}
