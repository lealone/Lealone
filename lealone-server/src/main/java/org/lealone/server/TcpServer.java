/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.server;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetEndpoint;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetServer;
import org.lealone.net.WritableChannel;

/**
 * The TCP server implements the native database server protocol.
 * It supports multiple client connections to multiple databases(many to many). 
 * The same database may be opened by multiple clients.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TcpServer extends DelegatedProtocolServer implements AsyncConnectionManager {

    private final CopyOnWriteArrayList<AsyncConnection> connections = new CopyOnWriteArrayList<>();

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public String getType() {
        return TcpServerEngine.NAME;
    }

    @Override
    public void init(Map<String, String> config) {
        if (!config.containsKey("port"))
            config.put("port", String.valueOf(Constants.DEFAULT_TCP_PORT));

        NetFactory factory = NetFactoryManager.getFactory(config);
        NetServer netServer = factory.createNetServer();
        netServer.setConnectionManager(this);
        setProtocolServer(netServer);
        netServer.init(config);

        NetEndpoint.setLocalTcpEndpoint(getHost(), getPort());
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
        ScheduleService.stop();
    }

    @Override
    public AsyncConnection createConnection(WritableChannel writableChannel, boolean isServer) {
        if (getAllowOthers() || allow(writableChannel.getHost())) {
            TcpServerConnection conn = new TcpServerConnection(writableChannel, isServer);
            conn.setBaseDir(getBaseDir());
            connections.add(conn);
            return conn;
        } else {
            // TODO
            // should support a list of allowed databases
            // and a list of allowed clients
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
