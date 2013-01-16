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
package com.codefollower.yourbase.server;

import java.sql.SQLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import com.codefollower.h2.engine.Constants;
import com.codefollower.h2.server.TcpServer;
import com.codefollower.h2.tools.Server;
import com.codefollower.yourbase.table.HBaseTableEngine;
import com.codefollower.yourbase.zookeeper.H2TcpPortTracker;
import com.codefollower.yourbase.zookeeper.ZooKeeperAdmin;

public class H2TcpServer {
    private static final Log log = LogFactory.getLog(H2TcpServer.class);

    private final int tcpPort;
    private final ServerName serverName;
    private final Server server;

    public H2TcpServer(HMaster master) {
        initDbSettings(master.getConfiguration());
        ZooKeeperAdmin.createBaseZNodes();
        tcpPort = getMasterTcpPort(master.getConfiguration());
        serverName = master.getServerName();
        server = createServer(master, null);
    }

    public H2TcpServer(HRegionServer regionServer) {
        initDbSettings(regionServer.getConfiguration());
        tcpPort = getRegionServerTcpPort(regionServer.getConfiguration());
        serverName = regionServer.getServerName();
        server = createServer(null, regionServer);
    }

    private void initDbSettings(Configuration conf) {
        System.setProperty("h2.defaultTableEngine", conf.get("h2.default.table.engine", HBaseTableEngine.class.getName()));
    }

    public static int getMasterTcpPort(Configuration conf) {
        return conf.getInt("h2.master.tcp.port", Constants.DEFAULT_TCP_PORT - 1);
    }

    public static int getRegionServerTcpPort(Configuration conf) {
        return conf.getInt("h2.regionserver.tcp.port", Constants.DEFAULT_TCP_PORT);
    }

    public void start() {
        try {
            server.start();
            H2TcpPortTracker.createH2TcpPortEphemeralNode(serverName, tcpPort);
            log.info("Started H2 database tcp server at port " + tcpPort);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            if (server != null)
                server.stop();
            log.info("H2 database tcp server stopped");
        } finally {
            H2TcpPortTracker.deleteH2TcpPortEphemeralNode(serverName, tcpPort);
        }
    }

    private Server createServer(HMaster master, HRegionServer regionServer) {
        String[] args = { "-tcp", "-tcpPort", "" + tcpPort };

        try {
            Server server = Server.createTcpServer(args);
            TcpServer tcpServer = (TcpServer) server.getService();
            tcpServer.setMaster(master);
            tcpServer.setRegionServer(regionServer);
            return server;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
