/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.client;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.MapUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.ConnectionSetting;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.db.scheduler.SchedulerFactoryBase;
import org.lealone.net.NetClient;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetScheduler;

public class ClientScheduler extends NetScheduler {

    private static final Logger logger = LoggerFactory.getLogger(ClientScheduler.class);

    // 杂七杂八的任务，数量不多，执行完就删除
    private final ConcurrentLinkedQueue<AsyncTask> miscTasks = new ConcurrentLinkedQueue<>();
    private final NetClient netClient;

    public ClientScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "CScheduleService-" + id,
                MapUtils.getInt(config, ConnectionSetting.NET_CLIENT_COUNT.name(), schedulerCount),
                config, false);
        NetFactory netFactory = NetFactoryManager.getFactory(config);
        netClient = netFactory.createNetClient();
        netEventLoop.setNetClient(netClient);
        getThread().setDaemon(true);
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
    public void handle(AsyncTask task) {
        miscTasks.add(task);
        wakeUp();
    }

    @Override
    protected void runMiscTasks() {
        runMiscTasks(miscTasks);
    }

    @Override
    public void run() {
        long lastTime = System.currentTimeMillis();
        while (!stopped) {
            runMiscTasks();
            runEventLoop();

            long currentTime = System.currentTimeMillis();
            if (currentTime - lastTime > 1000) {
                lastTime = currentTime;
                checkTimeout(currentTime);
            }
        }
        netEventLoop.close();
    }

    private void checkTimeout(long currentTime) {
        try {
            netClient.checkTimeout(currentTime);
        } catch (Throwable t) {
            logger.warn("Failed to checkTimeout", t);
        }
    }

    public static Scheduler getScheduler(ConnectionInfo ci, CaseInsensitiveMap<String> config) {
        return SchedulerFactoryBase.getScheduler(ClientScheduler.class.getName(), ci, config);
    }
}
