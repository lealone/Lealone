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
package com.codefollower.yourbase.hbase.server;

import java.net.Socket;
import java.sql.SQLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import com.codefollower.yourbase.engine.Constants;
import com.codefollower.yourbase.hbase.dbobject.table.HBaseTableEngine;
import com.codefollower.yourbase.hbase.zookeeper.TcpPortTracker;
import com.codefollower.yourbase.hbase.zookeeper.ZooKeeperAdmin;
import com.codefollower.yourbase.message.TraceSystem;
import com.codefollower.yourbase.server.TcpServer;
import com.codefollower.yourbase.server.TcpServerThread;

public class HBaseTcpServer extends TcpServer implements Runnable {
    private static final Log log = LogFactory.getLog(HBaseTcpServer.class);

    public static int getMasterTcpPort(Configuration conf) {
        return conf.getInt("yourbase.master.tcp.port", Constants.DEFAULT_TCP_PORT - 1);
    }

    public static int getRegionServerTcpPort(Configuration conf) {
        return conf.getInt("yourbase.regionserver.tcp.port", Constants.DEFAULT_TCP_PORT);
    }

    private final int tcpPort;
    private final ServerName serverName;

    private HMaster master;
    private HRegionServer regionServer;

    public HBaseTcpServer(HMaster master) {
        ZooKeeperAdmin.createBaseZNodes();
        initDbSettings(master.getConfiguration());
        tcpPort = getMasterTcpPort(master.getConfiguration());
        serverName = master.getServerName();
        this.master = master;
        init();
    }

    public HBaseTcpServer(HRegionServer regionServer) {
        initDbSettings(regionServer.getConfiguration());
        tcpPort = getRegionServerTcpPort(regionServer.getConfiguration());
        serverName = regionServer.getServerName();
        this.regionServer = regionServer;
        init();
    }

    public HMaster getMaster() {
        return master;
    }

    public HRegionServer getRegionServer() {
        return regionServer;
    }

    @Override
    public void start() {
        try {
            super.start();
            TcpPortTracker.createTcpPortEphemeralNode(serverName, tcpPort);

            String name = getName() + " (" + getURL() + ")";
            Thread t = new Thread(this, name);
            t.setDaemon(isDaemon());
            t.start();

            log.info("Started yourbase tcp server at port " + tcpPort);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        try {
            super.stop();
            log.info("Stopped yourbase tcp server");
        } finally {
            TcpPortTracker.deleteTcpPortEphemeralNode(serverName, tcpPort);
        }
    }

    @Override
    public String getName() {
        return "YourBase TCP Server";
    }

    @Override
    protected TcpServerThread createTcpServerThread(Socket socket, int threadId) {
        return new HBaseTcpServerThread(socket, this, threadId);
    }

    @Override
    protected void initManagementDb() throws SQLException {
        SERVERS.put(getPort(), this);
    }

    @Override
    protected synchronized void stopManagementDb() {
    }

    @Override
    protected synchronized void addConnection(int id, String url, String user) {
    }

    @Override
    protected synchronized void removeConnection(int id) {
    }

    private void init() {
        String[] args = { "-tcp", "-tcpPort", "" + tcpPort, "-tcpDaemon" };
        super.init(args);
    }

    private void initDbSettings(Configuration conf) {
        System.setProperty("yourbase.defaultTableEngine", conf.get("yourbase.default.table.engine", HBaseTableEngine.NAME));
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
