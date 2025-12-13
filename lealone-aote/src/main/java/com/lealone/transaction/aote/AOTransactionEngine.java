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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.util.ShutdownHookUtils;
import com.lealone.db.RunMode;
import com.lealone.db.SysProperties;
import com.lealone.db.plugin.PluginManager;
import com.lealone.db.scheduler.EmbeddedScheduler;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.db.scheduler.SchedulerFactoryBase;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageEventListener;
import com.lealone.storage.StorageMap;
import com.lealone.transaction.TransactionEngine;
import com.lealone.transaction.TransactionEngineBase;
import com.lealone.transaction.aote.log.LogSyncService;
import com.lealone.transaction.aote.tm.TransactionManager;

//Async adaptive Optimization Transaction Engine
public class AOTransactionEngine extends TransactionEngineBase implements StorageEventListener {

    private static final String NAME = "AOTE";
    private static final AtomicInteger logSyncServiceIndex = new AtomicInteger(0);

    private final AtomicLong lastTransactionId = new AtomicLong();

    // repeatable read 事务数
    private final AtomicInteger rrtCount = new AtomicInteger();

    private TransactionManager[] transactionManagers;

    private LogSyncService logSyncService; // 实际上就是logSyncServices[0]
    private LogSyncService[] logSyncServices;
    private SchedulerFactory schedulerFactory;

    public AOTransactionEngine() {
        super(NAME);
    }

    public LogSyncService getLogSyncService() {
        return logSyncService;
    }

    public LogSyncService[] getLogSyncServices() {
        return logSyncServices;
    }

    private int nextLogSyncServiceIndex() {
        return SchedulerFactoryBase.getAndIncrementIndex(logSyncServiceIndex) % logSyncServices.length;
    }

    @Override
    public void addGcTask(GcTask gcTask) {
        logSyncServices[nextLogSyncServiceIndex()].getCheckpointService().addGcTask(gcTask);
    }

    @Override
    public void removeGcTask(GcTask gcTask) {
        for (int i = 0; i < logSyncServices.length; i++) {
            logSyncServices[i].getCheckpointService().removeGcTask(gcTask);
        }
    }

    public void decrementRrtCount() {
        rrtCount.decrementAndGet();
    }

    @Override
    public boolean containsRepeatableReadTransactions() {
        return rrtCount.get() > 0;
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
    public void afterStorageMapOpen(StorageMap<?, ?> map) {
        if (logSyncService == null)
            return;
        // 内存表刷脏页时已经被过滤了
        int index = nextLogSyncServiceIndex();
        logSyncServices[index].getCheckpointService().addMap(map);

        // 内存表和索引不需要写redo log
        if (!map.isInMemory() && !map.getKeyType().isKeyOnly()) {
            logSyncServices[index].getRedoLog().addMap(map);
        }
    }

    public void removeStorageMap(String mapName) {
        for (int i = 0; i < logSyncServices.length; i++) {
            logSyncServices[i].getCheckpointService().removeMap(mapName);
            logSyncServices[i].getRedoLog().removeMap(mapName);
        }
    }

    ///////////////////// 实现TransactionEngine接口 /////////////////////

    @Override
    public synchronized void init(Map<String, String> config) {
        if (logSyncService != null)
            return;
        super.init(config);
        initServices();
        setGlobalShutdownHook();
    }

    private static void setGlobalShutdownHook() {
        ShutdownHookUtils.setGlobalShutdownHook(2, AOTransactionEngine.class, () -> {
            for (TransactionEngine te : PluginManager.getPlugins(TransactionEngine.class)) {
                te.close();
            }
        });
    }

    @Override
    public void close() {
        close(true);
    }

    public synchronized void close(boolean stopScheduler) {
        if (logSyncService == null)
            return;
        for (int i = 0; i < logSyncServices.length; i++) {
            if (logSyncServices[i].isRunning())
                logSyncServices[i].getCheckpointService().executeCheckpointAsync(true);
        }
        try {
            for (int i = 0; i < logSyncServices.length; i++) {
                if (logSyncServices[i].isRunning())
                    logSyncServices[i].close();
            }
        } catch (Exception e) {
        }
        if (stopScheduler) {
            try {
                schedulerFactory.stop();
            } catch (Exception e) {
            }
        }
        logSyncService = null;
        logSyncServices = null;
        schedulerFactory = null;
        super.close();
    }

    public long nextTransactionId() {
        return lastTransactionId.incrementAndGet();
    }

    @Override
    public AOTransaction beginTransaction(RunMode runMode, int isolationLevel,
            InternalScheduler scheduler) {
        if (logSyncService == null) {
            // 直接抛异常对上层很不友好，还不如用默认配置初始化
            init(getDefaultConfig());
        }
        long tid = nextTransactionId();
        AOTransaction t = createTransaction(tid, runMode, isolationLevel);
        if (t.isRepeatableRead())
            rrtCount.incrementAndGet();

        boolean isSingleThread = true;
        if (scheduler == null) {
            // 如果当前线程不是调度线程就给事务绑定一个Scheduler
            scheduler = (InternalScheduler) SchedulerThread.currentScheduler(schedulerFactory);
            if (scheduler == null) {
                scheduler = (InternalScheduler) schedulerFactory.getScheduler();
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
        for (int i = 0; i < logSyncServices.length; i++) {
            if (logSyncServices[i].isRunning())
                logSyncServices[i].getCheckpointService().executeCheckpointAsync(false);
        }
    }

    @Override
    public void recover(StorageMap<?, ?> map, List<StorageMap<?, ?>> indexMaps) {
        if (logSyncService == null)
            return;
        if (!map.isInMemory()) {
            logSyncService.getRedoLog().redo(map, indexMaps);
        }
    }

    @Override
    public Runnable getFsyncService() {
        return logSyncService;
    }

    private void initServices() {
        SchedulerFactory sf = SchedulerFactory.getDefaultSchedulerFactory();
        if (sf == null) {
            sf = SchedulerFactory.initDefaultSchedulerFactory(EmbeddedScheduler.class.getName(), config);
        }
        schedulerFactory = sf;
        int schedulerCount = schedulerFactory.getSchedulerCount();
        // 多加一个
        transactionManagers = new TransactionManager[schedulerCount + 1];
        for (int i = 0; i < schedulerCount; i++) {
            transactionManagers[i] = TransactionManager.create(this, true);
        }
        transactionManagers[schedulerCount] = TransactionManager.create(this, false);

        initLogSyncServices(schedulerCount);
    }

    private void initLogSyncServices(int schedulerCount) {
        logSyncService = LogSyncService.create(config);
        logSyncService.setCheckpointService(new CheckpointService(this, config, logSyncService));
        logSyncService.getRedoLog().setSyncServiceIndex(0);
        logSyncService.getRedoLog().init(); // 兼容老版本的redo log

        // 嵌入式场景需要启动logSyncService
        if (RunMode.isEmbedded(config)) {
            logSyncService.setName("FsyncService-0");
            logSyncService.start();
        }

        schedulerCount = Math.max(2, schedulerCount / 2); // 默认是scheduler的一半
        logSyncServices = new LogSyncService[schedulerCount];
        logSyncServices[0] = logSyncService;
        for (int i = 1; i < schedulerCount; i++) {
            logSyncServices[i] = LogSyncService.create(config);
            logSyncServices[i].setName("FsyncService-" + i);
            logSyncServices[i]
                    .setCheckpointService(new CheckpointService(this, config, logSyncServices[i]));
            logSyncServices[i].getRedoLog().setSyncServiceIndex(i);
            logSyncServices[i].start();
        }
    }
}
