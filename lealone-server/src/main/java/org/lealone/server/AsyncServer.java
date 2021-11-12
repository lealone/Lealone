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
import org.lealone.net.NetNode;
import org.lealone.net.NetServer;
import org.lealone.net.WritableChannel;

public abstract class AsyncServer<T extends AsyncConnection> extends DelegatedProtocolServer
        implements AsyncConnectionManager {

    private final CopyOnWriteArrayList<T> connections = new CopyOnWriteArrayList<>();

    @Override
    public void init(Map<String, String> config) {
        if (!config.containsKey("port"))
            config.put("port", String.valueOf(getDefaultPort()));
        if (!config.containsKey("__runInMainThread__"))
            config.put("__runInMainThread__", "true");
        if (!config.containsKey("name"))
            config.put("name", getName());

        NetFactory factory = NetFactoryManager.getFactory(config);
        NetServer netServer = factory.createNetServer();
        netServer.setConnectionManager(this);
        setProtocolServer(netServer);
        netServer.init(config);

        NetNode.setLocalTcpNode(getHost(), getPort());
        ScheduleService.init(config);
        ScheduleService.start(); // 提前启动，LealoneDatabase要用到存储引擎
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();

        for (T c : connections) {
            c.close();
        }
        ScheduleService.stop();
    }

    protected int getConnectionSize() {
        return connections.size();
    }

    protected abstract int getDefaultPort();

    protected abstract T createConnection(WritableChannel writableChannel, Scheduler scheduler);

    protected void onConnectionCreated(T conn, Scheduler scheduler) {
        // do nothing
    }

    @Override
    public T createConnection(WritableChannel writableChannel, boolean isServer) {
        if (getAllowOthers() || allow(writableChannel.getHost())) {
            Scheduler scheduler = ScheduleService.getSchedulerForSession();
            T conn = createConnection(writableChannel, scheduler);
            connections.add(conn);
            scheduler.register(conn);
            onConnectionCreated(conn, scheduler);
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
