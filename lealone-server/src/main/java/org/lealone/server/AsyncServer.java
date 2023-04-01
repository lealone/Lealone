/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetServer;
import org.lealone.net.WritableChannel;

public abstract class AsyncServer<T extends AsyncConnection> extends DelegatedProtocolServer
        implements AsyncConnectionManager {

    private final CopyOnWriteArrayList<T> connections = new CopyOnWriteArrayList<>();

    @Override
    public void init(Map<String, String> config) {
        if (!config.containsKey("port"))
            config.put("port", String.valueOf(getDefaultPort()));
        if (!config.containsKey("name"))
            config.put("name", getName());

        NetFactory factory = NetFactoryManager.getFactory(config);
        NetServer netServer = factory.createNetServer();
        netServer.setConnectionManager(this);
        setProtocolServer(netServer);
        netServer.init(config);
        SchedulerFactory.init(config);
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        super.start();
        ProtocolServerEngine.startedServers.add(this);
    }

    @Override
    public void stop() {
        synchronized (this) {
            if (isStopped())
                return;
            super.stop();
            // 不关闭连接，执行SHUTDOWN SERVER后只是不能创建新连接
            // for (T c : connections) {
            // c.close();
            // }
            ProtocolServerEngine.startedServers.remove(this);
        }
        // 同步块不能包含下面的代码，否则执行System.exit时会触发ShutdownHook又调用到stop，
        // 而System.exit又需要等所有ShutdownHook结束后才能退出，所以就死锁了
        if (ProtocolServerEngine.startedServers.isEmpty()) {
            SchedulerFactory.destroy();
            System.exit(0);
        }
    }

    protected int getConnectionSize() {
        return connections.size();
    }

    protected abstract int getDefaultPort();

    protected abstract T createConnection(WritableChannel writableChannel, Scheduler scheduler);

    protected void beforeRegister(T conn, Scheduler scheduler) {
        // do nothing
    }

    protected void afterRegister(T conn, Scheduler scheduler) {
        // do nothing
    }

    @Override
    public T createConnection(WritableChannel writableChannel, boolean isServer) {
        if (getAllowOthers() || allow(writableChannel.getHost())) {
            Scheduler scheduler = SchedulerFactory.getScheduler();
            T conn = createConnection(writableChannel, scheduler);
            connections.add(conn);
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
        connections.remove(conn);
        conn.close();
    }
}
