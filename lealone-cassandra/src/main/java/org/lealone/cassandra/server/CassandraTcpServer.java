/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.lealone.cassandra.server;

import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lealone.cassandra.config.CassandraConfig;
import org.lealone.message.TraceSystem;
import org.lealone.server.TcpServer;
import org.lealone.server.TcpServerThread;

public class CassandraTcpServer extends TcpServer implements Runnable {
    private static final Log log = LogFactory.getLog(CassandraTcpServer.class);

    private final CassandraConfig config;

    public CassandraTcpServer(CassandraConfig config) {
        this.config = config;
        init();
    }

    @Override
    public void start() {
        try {
            super.start();
            String name = getName() + " (" + getURL() + ")";
            Thread t = new Thread(this, name);
            t.setDaemon(isDaemon());
            t.start();

            log.info("Started lealone tcp server at port " + getPort());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        super.stop();
        log.info("Stopped lealone tcp server");

    }

    @Override
    public String getName() {
        return "Lealone tcp server";
    }

    @Override
    protected TcpServerThread createTcpServerThread(Socket socket, int threadId) {
        return new CassandraTcpServerThread(socket, this, threadId);
    }

    @Override
    protected void initManagementDb() throws SQLException {
        SERVERS.put(getPort(), this);
    }

    @Override
    protected void stopManagementDb() {
    }

    @Override
    protected void addConnection(int id, String url, String user) {
    }

    @Override
    protected void removeConnection(int id) {
    }

    private void init() {
        ArrayList<String> args = new ArrayList<String>();
        args.add("-tcpPort");
        args.add("" + config.lealone_tcp_port);
        super.init(args.toArray(new String[0]));
    }

    @Override
    public void run() {
        try {
            listen();
        } catch (Exception e) {
            TraceSystem.traceThrowable(e);
        }
    }
}
