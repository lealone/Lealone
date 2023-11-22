/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.client;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.MapUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.ConnectionSetting;
import org.lealone.db.PluginManager;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.scheduler.ISessionInfo;
import org.lealone.db.scheduler.ISessionInitTask;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.db.scheduler.SchedulerFactory;
import org.lealone.net.NetClient;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetScheduler;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.storage.page.PageOperation;
import org.lealone.transaction.PendingTransaction;

public class ClientScheduler extends NetScheduler {

    private static final Logger logger = LoggerFactory.getLogger(ClientScheduler.class);

    // 杂七杂八的任务，数量不多，执行完就删除
    private final ConcurrentLinkedQueue<AsyncTask> miscTasks = new ConcurrentLinkedQueue<>();
    private final NetClient netClient;

    public ClientScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "CScheduleService-" + id,
                MapUtils.getInt(config, ConnectionSetting.NET_CLIENT_COUNT.name(), schedulerCount),
                config);
        NetFactory netFactory = NetFactoryManager.getFactory(config);
        netClient = netFactory.createNetClient();
        netEventLoop.setNetClient(netClient);
        getThread().setDaemon(true);
    }

    @Override
    public long getLoad() {
        return super.getLoad() + miscTasks.size();
    }

    @Override
    public Logger getLogger() {
        return logger;
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
    public void handlePageOperation(PageOperation po) {
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
        wakeUp();
    }

    @Override
    public void addTransaction(PendingTransaction pt) {
    }

    @Override
    public PendingTransaction getTransaction() {
        return null;
    }

    @Override
    public void run() {
        long lastTime = System.currentTimeMillis();
        while (!stopped) {
            runMiscTasks();
            runPendingTasks();
            runEventLoop();

            long currentTime = System.currentTimeMillis();
            if (currentTime - lastTime > 1000) {
                lastTime = currentTime;
                checkTimeout(currentTime);
            }
        }
        netEventLoop.close();
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

    private void checkTimeout(long currentTime) {
        try {
            netClient.checkTimeout(currentTime);
        } catch (Throwable t) {
            logger.warn("Failed to checkTimeout", t);
        }
    }

    @Override
    public void registerConnectOperation(SocketChannel channel, Object attachment)
            throws ClosedChannelException {
        try {
            channel.register(netEventLoop.getSelector(), SelectionKey.OP_CONNECT, attachment);
        } catch (ClosedChannelException e) {
            netEventLoop.closeChannel(channel);
            throw e;
        }
    }

    private static SchedulerFactory defaultSchedulerFactory;

    public static void setDefaultSchedulerFactory(SchedulerFactory defaultSchedulerFactory) {
        ClientScheduler.defaultSchedulerFactory = defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory() {
        return defaultSchedulerFactory;
    }

    public static SchedulerFactory getDefaultSchedulerFactory(Properties prop) {
        if (ClientScheduler.defaultSchedulerFactory == null) {
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
        if (ClientScheduler.defaultSchedulerFactory == null)
            initDefaultSchedulerFactory(config);
        return defaultSchedulerFactory;
    }

    public static synchronized SchedulerFactory initDefaultSchedulerFactory(Map<String, String> config) {
        SchedulerFactory schedulerFactory = ClientScheduler.defaultSchedulerFactory;
        if (schedulerFactory == null) {
            String sf = MapUtils.getString(config, "scheduler_factory", null);
            if (sf != null) {
                schedulerFactory = PluginManager.getPlugin(SchedulerFactory.class, sf);
            } else {
                ClientScheduler[] schedulers = createSchedulers(config);
                schedulerFactory = SchedulerFactory.create(config, schedulers);
            }
            if (!schedulerFactory.isInited())
                schedulerFactory.init(config);
            ClientScheduler.defaultSchedulerFactory = schedulerFactory;
        }
        return schedulerFactory;
    }

    public static Scheduler getScheduler(ConnectionInfo ci, CaseInsensitiveMap<String> config) {
        Scheduler scheduler = ci.getScheduler();
        if (scheduler == null) {
            SchedulerFactory sf = ClientScheduler.getDefaultSchedulerFactory(config);
            scheduler = sf.getScheduler();
            ci.setScheduler(scheduler);
            if (!sf.isStarted())
                sf.start();
        }
        return scheduler;
    }

    // --------------------- 创建所有的调度器 ---------------------

    public static ClientScheduler[] createSchedulers(Map<String, String> config) {
        int schedulerCount = MapUtils.getSchedulerCount(config);
        ClientScheduler[] schedulers = new ClientScheduler[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            schedulers[i] = new ClientScheduler(i, schedulerCount, config);
        }
        return schedulers;
    }
}
