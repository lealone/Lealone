/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MapUtils;
import org.lealone.db.PluginManager;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.db.scheduler.SchedulerFactory;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetServer;
import org.lealone.net.WritableChannel;

public abstract class AsyncServer<T extends AsyncConnection> extends DelegatedProtocolServer
        implements AsyncConnectionManager {

    private static SchedulerFactory schedulerFactory;

    public static SchedulerFactory getSchedulerFactory() {
        return schedulerFactory;
    }

    public static synchronized void initSchedulerFactory(Map<String, String> config) {
        SchedulerFactory schedulerFactory = AsyncServer.schedulerFactory;
        if (schedulerFactory == null) {
            String sf = MapUtils.getString(config, "scheduler_factory", null);
            if (sf != null) {
                schedulerFactory = PluginManager.getPlugin(SchedulerFactory.class, sf);
            } else {
                GlobalScheduler[] schedulers = GlobalScheduler.createSchedulers(config);
                schedulerFactory = SchedulerFactory.create(config, schedulers);
            }
            if (!schedulerFactory.isInited())
                schedulerFactory.init(config);
            AsyncServer.schedulerFactory = schedulerFactory;
        }
    }

    private final AtomicInteger connectionSize = new AtomicInteger();
    private int serverId;

    public int getServerId() {
        return serverId;
    }

    @Override
    public void init(Map<String, String> config) {
        if (!config.containsKey("port"))
            config.put("port", String.valueOf(getDefaultPort()));
        if (!config.containsKey("name"))
            config.put("name", getName());
        serverId = AsyncServerManager.allocateServerId();
        AsyncServerManager.addServer(this);

        NetFactory factory = NetFactoryManager.getFactory(config);
        NetServer netServer = factory.createNetServer();
        netServer.setConnectionManager(this);
        setProtocolServer(netServer);
        netServer.init(config);

        initSchedulerFactory(config);
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        super.start();
        ProtocolServerEngine.startedServers.add(this);
        // 等所有的Server启动完成后再在Lealone.main中调用SchedulerFactory.start
        // 确保所有的初始PeriodicTask都在main线程中注册
    }

    @Override
    public void stop() {
        synchronized (this) {
            if (isStopped())
                return;
            super.stop();
            ProtocolServerEngine.startedServers.remove(this);
            AsyncServerManager.removeServer(this);
        }
        // 同步块不能包含下面的代码，否则执行System.exit时会触发ShutdownHook又调用到stop，
        // 而System.exit又需要等所有ShutdownHook结束后才能退出，所以就死锁了
        if (ProtocolServerEngine.startedServers.isEmpty()) {
            schedulerFactory.stop();
            // 如果当前线程是ShutdownHook不能再执行System.exit，否则无法退出
            if (!Thread.currentThread().getName().contains("ShutdownHook"))
                System.exit(0);
        }
    }

    protected int getConnectionSize() {
        return connectionSize.get();
    }

    protected abstract int getDefaultPort();

    protected T createConnection(WritableChannel writableChannel, Scheduler scheduler) {
        throw DbException.getUnsupportedException("createConnection");
    }

    protected void beforeRegister(T conn, Scheduler scheduler) {
        // do nothing
    }

    protected void afterRegister(T conn, Scheduler scheduler) {
        // do nothing
    }

    @Override
    public T createConnection(WritableChannel writableChannel, boolean isServer, Scheduler scheduler) {
        if (getAllowOthers() || allow(writableChannel.getHost())) {
            T conn = createConnection(writableChannel, scheduler);
            connectionSize.incrementAndGet();
            beforeRegister(conn, scheduler);
            scheduler.register(conn);
            afterRegister(conn, scheduler);
            return conn;
        } else {
            writableChannel.close();
            throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
        }
    }

    @Override
    public void removeConnection(AsyncConnection conn) {
        connectionSize.decrementAndGet();
        conn.close();
    }

    @Override
    public void registerAccepter(ServerSocketChannel serverChannel) {
        Scheduler scheduler = schedulerFactory.getScheduler();
        scheduler.registerAccepter(this, serverChannel);
    }

    public boolean isRoundRobinAcceptEnabled() {
        return true;
    }
}
