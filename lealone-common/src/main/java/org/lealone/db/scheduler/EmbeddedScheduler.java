/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.MapUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.PluginManager;
import org.lealone.db.async.AsyncTask;
import org.lealone.server.ProtocolServer;
import org.lealone.sql.PreparedSQLStatement;

public class EmbeddedScheduler extends SchedulerBase {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedScheduler.class);
    private final Semaphore semaphore = new Semaphore(1);
    private final AtomicBoolean waiting = new AtomicBoolean(false);
    private volatile boolean haveWork;

    // 杂七杂八的任务，数量不多，执行完就删除
    private final ConcurrentLinkedQueue<AsyncTask> miscTasks = new ConcurrentLinkedQueue<>();

    public EmbeddedScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "EScheduleService-" + id, schedulerCount, config);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public long getLoad() {
        return super.getLoad() + miscTasks.size();
    }

    @Override
    public void wakeUp() {
        haveWork = true;
        if (waiting.compareAndSet(true, false)) {
            semaphore.release(1);
        }
    }

    @Override
    public void run() {
        while (!stopped) {
            runMiscTasks();
            runPageOperationTasks();
            runPendingTransactions();
            runPendingTasks();
            doAwait();
        }
    }

    private void doAwait() {
        if (waiting.compareAndSet(false, true)) {
            if (haveWork) {
                haveWork = false;
            } else {
                try {
                    semaphore.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
                    semaphore.drainPermits();
                } catch (InterruptedException e) {
                    logger.warn("", e);
                }
            }
            waiting.set(false);
        }
    }

    private void runMiscTasks() {
        if (!miscTasks.isEmpty()) {
            AsyncTask task = miscTasks.poll();
            while (task != null) {
                try {
                    task.run();
                } catch (Throwable e) {
                    logger.warn("Failed to run misc task: " + task, e);
                }
                task = miscTasks.poll();
            }
        }
    }

    @Override
    public void executeNextStatement() {
    }

    @Override
    public boolean yieldIfNeeded(PreparedSQLStatement current) {
        return false;
    }

    @Override
    public void handle(AsyncTask task) {
        miscTasks.add(task);
    }

    @Override
    public void operationUndo() {
    }

    @Override
    public void operationComplete() {
    }

    @Override
    public Selector getSelector() {
        return null;
    }

    @Override
    public void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel) {
    }

    @Override
    public void register(Object conn) {
    }

    @Override
    public void addSessionInitTask(ISessionInitTask task) {
    }

    @Override
    public void addSessionInfo(ISessionInfo si) {
    }

    @Override
    public void removeSessionInfo(ISessionInfo si) {

    }

    @Override
    public void validateSession(boolean isUserAndPasswordCorrect) {
    }

    @Override
    public void await() {
        for (;;) {
            if (syncCounter.get() < 1)
                break;
            runPageOperationTasks();
            runPendingTransactions();
            if (syncCounter.get() < 1)
                break;
        }
        needWakeUp = true;
        if (syncException != null)
            throw syncException;
    }

    // --------------------- 创建所有的调度器 ---------------------

    public static EmbeddedScheduler[] createSchedulers(Map<String, String> config) {
        int schedulerCount = MapUtils.getSchedulerCount(config);
        EmbeddedScheduler[] schedulers = new EmbeddedScheduler[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            schedulers[i] = new EmbeddedScheduler(i, schedulerCount, config);
        }
        return schedulers;
    }

    private static SchedulerFactory defaultSchedulerFactory;

    public static void setDefaultSchedulerFactory(SchedulerFactory defaultSchedulerFactory) {
        EmbeddedScheduler.defaultSchedulerFactory = defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory() {
        return defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory(Properties prop) {
        if (EmbeddedScheduler.defaultSchedulerFactory == null) {
            Map<String, String> config;
            if (prop != null)
                config = new CaseInsensitiveMap<>(prop);
            else
                config = new CaseInsensitiveMap<>();
            initDefaultSchedulerFactory(config);
        }
        return defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory(Map<String, String> config) {
        if (EmbeddedScheduler.defaultSchedulerFactory == null)
            initDefaultSchedulerFactory(config);
        return defaultSchedulerFactory;
    }

    public static synchronized SchedulerFactory initDefaultSchedulerFactory(Map<String, String> config) {
        SchedulerFactory schedulerFactory = EmbeddedScheduler.defaultSchedulerFactory;
        if (schedulerFactory == null) {
            String sf = MapUtils.getString(config, "scheduler_factory", null);
            if (sf != null) {
                schedulerFactory = PluginManager.getPlugin(SchedulerFactory.class, sf);
            } else {
                EmbeddedScheduler[] schedulers = createSchedulers(config);
                schedulerFactory = SchedulerFactory.create(config, schedulers);
            }
            if (!schedulerFactory.isInited())
                schedulerFactory.init(config);
            EmbeddedScheduler.defaultSchedulerFactory = schedulerFactory;
        }
        return schedulerFactory;
    }

    public static Scheduler getScheduler(ConnectionInfo ci) {
        Scheduler scheduler = ci.getScheduler();
        if (scheduler == null) {
            SchedulerFactory sf = EmbeddedScheduler.getDefaultSchedulerFactory(ci.getProperties());
            scheduler = sf.getScheduler();
            ci.setScheduler(scheduler);
            if (!sf.isStarted())
                sf.start();
        }
        return scheduler;
    }
}
