/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server;

import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.net.AsyncConnection;
import com.lealone.net.AsyncConnectionManager;
import com.lealone.net.NetEventLoop;
import com.lealone.net.NetFactory;
import com.lealone.net.NetServer;
import com.lealone.net.WritableChannel;
import com.lealone.server.scheduler.GlobalScheduler;
import com.lealone.transaction.TransactionEngine;

public abstract class AsyncServer<T extends AsyncConnection> extends DelegatedProtocolServer
        implements AsyncConnectionManager {

    private final AtomicInteger connectionSize = new AtomicInteger();
    private SchedulerFactory schedulerFactory;
    private int serverId;

    public SchedulerFactory getSchedulerFactory() {
        return schedulerFactory;
    }

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

        NetFactory factory = NetFactory.getFactory(config);
        NetServer netServer = factory.createNetServer();
        netServer.setConnectionManager(this);
        setProtocolServer(netServer);
        netServer.init(config);

        schedulerFactory = SchedulerFactory.initDefaultSchedulerFactory(GlobalScheduler.class.getName(),
                config);
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
            TransactionEngine.getDefaultTransactionEngine().close();
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

    protected void register(T conn, Scheduler scheduler) {
        beforeRegister(conn, scheduler);
        NetEventLoop eventLoop = (NetEventLoop) scheduler.getNetEventLoop();
        eventLoop.register(conn);
        afterRegister(conn, scheduler);
    }

    @Override
    public T createConnection(WritableChannel writableChannel, boolean isServer, Scheduler scheduler) {
        if (getAllowOthers() || allow(writableChannel.getHost())) {
            T conn = createConnection(writableChannel, scheduler);
            connectionSize.incrementAndGet();
            register(conn, scheduler);
            // 当前调度器接入一条新连接后可以选择是否让其他调度器负责监听Accept事件
            AsyncServerManager.registerAccepterIfNeed(this, scheduler);
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
        // 挑选出第一个负责监听Accept事件的调度器
        Scheduler scheduler = schedulerFactory.getScheduler();
        registerAccepter(serverChannel, scheduler);
    }

    protected void registerAccepter(ServerSocketChannel serverChannel, Scheduler scheduler) {
        this.serverChannel = serverChannel;
        AsyncServerManager.registerAccepter(this, scheduler);
        scheduler.wakeUp();
    }

    public boolean isRoundRobinAcceptEnabled() {
        return true;
    }

    private ServerSocketChannel serverChannel;

    public ServerSocketChannel getServerChannel() {
        return serverChannel;
    }

    private SelectionKey selectionKey;

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    public void setSelectionKey(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }
}
