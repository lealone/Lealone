/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.server;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetNode;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetServer;
import org.lealone.net.WritableChannel;

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
        config.put("__runInMainThread__", "true");

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
    public boolean runInMainThread() {
        return protocolServer.runInMainThread();
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
            TcpServerConnection conn = new TcpServerConnection(this, writableChannel, isServer);
            connections.add(conn);
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
