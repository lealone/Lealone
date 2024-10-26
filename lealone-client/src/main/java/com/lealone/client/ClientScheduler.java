/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.MapUtils;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.DataBufferFactory;
import com.lealone.db.async.AsyncTask;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerBase;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.db.scheduler.SchedulerFactoryBase;
import com.lealone.net.NetClient;
import com.lealone.net.NetEventLoop;
import com.lealone.net.NetFactory;
import com.lealone.server.ProtocolServer;

public class ClientScheduler extends SchedulerBase {

    private static final Logger logger = LoggerFactory.getLogger(ClientScheduler.class);

    // 杂七杂八的任务，数量不多，执行完就删除
    private final ConcurrentLinkedQueue<AsyncTask> miscTasks = new ConcurrentLinkedQueue<>();
    private final NetClient netClient;
    private final NetEventLoop netEventLoop;

    public ClientScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "CScheduleService-" + id,
                MapUtils.getInt(config, ConnectionSetting.NET_CLIENT_COUNT.name(), schedulerCount),
                config);
        NetFactory netFactory = NetFactory.getFactory(config);
        netClient = netFactory.createNetClient();
        netEventLoop = netFactory.createNetEventLoop(loopInterval, false);
        netEventLoop.setScheduler(this);
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
        onStopped();
    }

    private void checkTimeout(long currentTime) {
        try {
            netClient.checkTimeout(currentTime);
        } catch (Throwable t) {
            logger.warn("Failed to checkTimeout", t);
        }
    }

    @Override
    public void executeNextStatement() {
        // 客户端阻塞在同步方法时运行事件循环执行回调
        runEventLoop();
    }

    // --------------------- 网络事件循环 ---------------------

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return netEventLoop.getDataBufferFactory();
    }

    @Override
    public NetEventLoop getNetEventLoop() {
        return netEventLoop;
    }

    @Override
    public Selector getSelector() {
        return netEventLoop.getSelector();
    }

    @Override
    public void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel) {
        DbException.throwInternalError();
    }

    @Override
    public void wakeUp() {
        netEventLoop.wakeup();
    }

    @Override
    protected void runEventLoop() {
        try {
            netEventLoop.write();
            netEventLoop.select();
            netEventLoop.handleSelectedKeys();
        } catch (Throwable t) {
            getLogger().warn("Failed to runEventLoop", t);
        }
    }

    @Override
    protected void onStopped() {
        super.onStopped();
        netEventLoop.close();
    }

    private static volatile SchedulerFactory clientSchedulerFactory;

    public static Scheduler getScheduler(ConnectionInfo ci, Map<String, String> config) {
        if (clientSchedulerFactory == null) {
            synchronized (ClientScheduler.class) {
                if (clientSchedulerFactory == null) {
                    clientSchedulerFactory = SchedulerFactoryBase
                            .createSchedulerFactory(ClientScheduler.class.getName(), config);
                }
            }
        }
        return SchedulerFactoryBase.getScheduler(clientSchedulerFactory, ci);
    }
}
