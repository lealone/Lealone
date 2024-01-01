/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.util.ShutdownHookUtils;
import com.lealone.db.RunMode;
import com.lealone.db.SysProperties;
import com.lealone.db.scheduler.EmbeddedScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageEventListener;
import com.lealone.storage.StorageMap;
import com.lealone.transaction.TransactionEngineBase;
import com.lealone.transaction.aote.log.LogSyncService;
import com.lealone.transaction.aote.log.RedoLogRecord;
import com.lealone.transaction.aote.tm.TransactionManager;

//Async adaptive Optimization Transaction Engine
public class AOTransactionEngine extends TransactionEngineBase implements StorageEventListener {

    private static final String NAME = "AOTE";

    private final AtomicLong lastTransactionId = new AtomicLong();

    // repeatable read 事务数
    private final AtomicInteger rrTransactionCount = new AtomicInteger();

    private TransactionManager[] transactionManagers;

    private LogSyncService logSyncService;
    private CheckpointService masterCheckpointService;
    CheckpointService[] checkpointServices;
    SchedulerFactory schedulerFactory;

    public AOTransactionEngine() {
        super(NAME);
    }

    public LogSyncService getLogSyncService() {
        return logSyncService;
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

    public void decrementRrtCount() {
        rrTransactionCount.decrementAndGet();
    }

    @Override
    public boolean containsRepeatableReadTransactions() {
        return rrTransactionCount.get() > 0;
    }

    public long getMaxRepeatableReadTransactionId() {
        long maxTid = -1;
        for (AOTransaction t : currentTransactions()) {
            if (t.isRepeatableRead() && t.getTransactionId() > maxTid)
                maxTid = t.getTransactionId();
        }
        return maxTid;
    }

    @Override
    public List<AOTransaction> currentTransactions() {
        // 还没有初始化时直接返回emptyList
        if (transactionManagers == null)
            return Collections.emptyList();
        int length = transactionManagers.length;
        int count = 0;
        for (int i = 0; i < length; i++) {
            count += transactionManagers[i].currentTransactionCount();
        }
        if (count == 0)
            return Collections.emptyList();
        ArrayList<AOTransaction> list = new ArrayList<>(count);
        for (int i = 0; i < length; i++) {
            transactionManagers[i].currentTransactions(list);
        }
        return list;
    }

    ///////////////////// 实现StorageEventListener接口 /////////////////////

    @Override
    public synchronized void beforeClose(Storage storage) {
        // 事务引擎已经关闭了，此时忽略存储引擎的事件响应
        if (logSyncService == null)
            return;
        checkpoint();
        for (String mapName : storage.getMapNames()) {
            removeStorageMap(mapName);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterStorageMapOpen(StorageMap<?, ?> map) {
        if (!map.isInMemory()) {
            logSyncService.getRedoLog().redo((StorageMap<Object, Object>) map);
        }
        Scheduler scheduler = schedulerFactory.getScheduler();
        checkpointServices[scheduler.getId()].addMap(map);
    }

    void removeStorageMap(AOTransaction transaction, String mapName) {
        RedoLogRecord r = RedoLogRecord.createDroppedMapRedoLogRecord(mapName);
        logSyncService.syncWrite(transaction, r, logSyncService.nextLogId());
        removeStorageMap(mapName);
    }

    private void removeStorageMap(String mapName) {
        for (int i = 0; i < checkpointServices.length; i++) {
            checkpointServices[i].removeMap(mapName);
        }
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
    public AOTransaction beginTransaction(RunMode runMode, int isolationLevel, Scheduler scheduler) {
        if (logSyncService == null) {
            // 直接抛异常对上层很不友好，还不如用默认配置初始化
            init(getDefaultConfig());
        }
        long tid = nextTransactionId();
        AOTransaction t = createTransaction(tid, runMode, isolationLevel);
        if (t.isRepeatableRead())
            rrTransactionCount.incrementAndGet();

        boolean isSingleThread = true;
        if (scheduler == null) {
            // 如果当前线程不是调度线程就给事务绑定一个Scheduler
            scheduler = SchedulerThread.currentScheduler(schedulerFactory);
            if (scheduler == null) {
                scheduler = schedulerFactory.getScheduler();
                isSingleThread = false;
            }
        }
        t.setScheduler(scheduler);
        TransactionManager tm;
        if (isSingleThread) {
            tm = transactionManagers[scheduler.getId()];
        } else {
            tm = transactionManagers[transactionManagers.length - 1];
        }
        t.setTransactionManager(tm);
        tm.addTransaction(t);
        return t;
    }

    private static Map<String, String> getDefaultConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("base_dir", SysProperties.getBaseDir());
        config.put("redo_log_dir", "redo_log");
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        return config;
    }

    private AOTransaction createTransaction(long tid, RunMode runMode, int isolationLevel) {
        return new AOTransaction(this, tid, runMode, isolationLevel);
    }

    @Override
    public boolean supportsMVCC() {
        return true;
    }

    @Override
    public void checkpoint() {
        masterCheckpointService.executeCheckpointAsync();
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
            checkpointServices[i] = new CheckpointService(this, config, schedulers[i]);
        }
        masterCheckpointService = checkpointServices[0];

        // 多加一个
        transactionManagers = new TransactionManager[schedulerCount + 1];
        for (int i = 0; i < schedulerCount; i++) {
            transactionManagers[i] = TransactionManager.create(this, true);
        }
        transactionManagers[schedulerCount] = TransactionManager.create(this, false);
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
