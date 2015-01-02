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
package org.lealone.hbase.server;

import static org.lealone.hbase.engine.HBaseConstants.DEFAULT_MASTER_TCP_PORT;
import static org.lealone.hbase.engine.HBaseConstants.DEFAULT_REGIONSERVER_TCP_PORT;
import static org.lealone.hbase.engine.HBaseConstants.DEFAULT_TABLE_ENGINE;
import static org.lealone.hbase.engine.HBaseConstants.DEFAULT_TCP_SERVER_START_ARGS;
import static org.lealone.hbase.engine.HBaseConstants.MASTER_TCP_PORT;
import static org.lealone.hbase.engine.HBaseConstants.REGIONSERVER_TCP_PORT;
import static org.lealone.hbase.engine.HBaseConstants.TCP_SERVER_START_ARGS;

import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.lealone.engine.Constants;
import org.lealone.hbase.dbobject.table.HBaseTableEngine;
import org.lealone.hbase.zookeeper.TcpPortTracker;
import org.lealone.hbase.zookeeper.ZooKeeperAdmin;
import org.lealone.message.TraceSystem;
import org.lealone.server.TcpServer;
import org.lealone.server.TcpServerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTcpServer extends TcpServer implements Runnable, HBaseServer {
    private static final Logger logger = LoggerFactory.getLogger(HBaseTcpServer.class);

    public static int getMasterTcpPort(Configuration conf) {
        return conf.getInt(MASTER_TCP_PORT, DEFAULT_MASTER_TCP_PORT);
    }

    public static int getRegionServerTcpPort(Configuration conf) {
        return conf.getInt(REGIONSERVER_TCP_PORT, DEFAULT_REGIONSERVER_TCP_PORT);
    }

    private final int tcpPort;
    private final ServerName serverName;

    private HMaster master;
    private HRegionServer regionServer;

    public HBaseTcpServer(HMaster master) {
        ZooKeeperAdmin.createBaseZNodes();
        initConf(master.getConfiguration());
        tcpPort = getMasterTcpPort(master.getConfiguration());
        serverName = master.getServerName();
        this.master = master;
        init(master.getConfiguration());
    }

    public HBaseTcpServer(HRegionServer regionServer) {
        ZooKeeperAdmin.createBaseZNodes();
        initConf(regionServer.getConfiguration());
        tcpPort = getRegionServerTcpPort(regionServer.getConfiguration());
        serverName = regionServer.getServerName();
        this.regionServer = regionServer;
        init(regionServer.getConfiguration());
    }

    @Override
    public HMaster getMaster() {
        return master;
    }

    @Override
    public HRegionServer getRegionServer() {
        return regionServer;
    }

    @Override
    public void start() {
        try {
            super.start();
            TcpPortTracker.createTcpPortEphemeralNode(serverName, tcpPort, master != null);
            ZooKeeperAdmin.getTcpPortTracker(); //初始化TcpPortTracker

            String name = getName() + " (" + getURL() + ")";
            Thread t = new Thread(this, name);
            t.setDaemon(isDaemon());
            t.start();

            logger.info("Lealone TcpServer started, listening port: {}", tcpPort);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        try {
            super.stop();
            logger.info("Lealone TcpServer stoppedr");
        } finally {
            TcpPortTracker.deleteTcpPortEphemeralNode(serverName, tcpPort, master != null);
        }
    }

    @Override
    protected TcpServerThread createTcpServerThread(Socket socket, int threadId) {
        return new HBaseTcpServerThread(socket, this, threadId);
    }

    private void init(Configuration conf) {
        ArrayList<String> args = new ArrayList<String>();
        for (String arg : conf.getStrings(TCP_SERVER_START_ARGS, DEFAULT_TCP_SERVER_START_ARGS)) {
            int pos = arg.indexOf('=');
            if (pos == -1) {
                args.add(arg.trim());
            } else {
                args.add(arg.substring(0, pos).trim());
                args.add(arg.substring(pos + 1).trim());
            }
        }
        args.add("-tcpPort");
        args.add("" + tcpPort);
        super.init(args.toArray(new String[0]));
    }

    private void initConf(Configuration conf) {
        String key;
        for (Entry<String, String> e : conf) {
            key = e.getKey().toLowerCase();
            if (key.startsWith(Constants.PROJECT_NAME_PREFIX)) {
                System.setProperty(key, e.getValue());
            }
        }
        key = DEFAULT_TABLE_ENGINE;
        System.setProperty(key, conf.get(key, HBaseTableEngine.NAME));
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
