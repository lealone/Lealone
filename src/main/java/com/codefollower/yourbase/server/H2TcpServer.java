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
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.h2.server.Service;
import org.h2.tools.Server;

import com.codefollower.yourbase.coprocessor.HBaseMasterObserver;
import com.codefollower.yourbase.zookeeper.H2TcpPortTracker;

public class H2TcpServer {
	private static final Log log = LogFactory.getLog(HBaseMasterObserver.class.getName());
	
    private Server server;
    private int tcpPort;
    private ServerName serverName;

    public static int getMasterTcpPort(Configuration conf) {
        return conf.getInt("h2.master.tcp.port", org.h2.engine.Constants.DEFAULT_TCP_PORT - 1);
    }

    public static int getRegionServerTcpPort(Configuration conf) {
        return conf.getInt("h2.regionserver.tcp.port", org.h2.engine.Constants.DEFAULT_TCP_PORT);
    }

    public void stop() {
        if (server != null)
            server.stop();

        H2TcpPortTracker.deleteH2TcpPortEphemeralNode(serverName, tcpPort);
    }

    public void start(int tcpPort, HMaster master) {
        this.serverName = master.getServerName();
        this.tcpPort = tcpPort;
        startServer(tcpPort, master, null);
        H2TcpPortTracker.createH2TcpPortEphemeralNode(master.getServerName(), tcpPort);
    }

    public void start(int tcpPort, HRegionServer regionServer) {
        this.serverName = regionServer.getServerName();
        this.tcpPort = tcpPort;
        startServer(tcpPort, null, regionServer);
        H2TcpPortTracker.createH2TcpPortEphemeralNode(regionServer.getServerName(), tcpPort);
    }

    private void startServer(int tcpPort, HMaster master, HRegionServer regionServer) {
        ArrayList<String> list = new ArrayList<String>();
        list.add("-tcp");
        list.add("-tcpPort");
        list.add(Integer.toString(tcpPort));

        try {
            server = Server.createTcpServer(list.toArray(new String[list.size()]));
            Service service = server.getService();
            org.h2.server.TcpServer tcpServer = (org.h2.server.TcpServer) service;
            tcpServer.setMaster(master);
            tcpServer.setRegionServer(regionServer);
            server.start();

            log.info("Started H2 database tcp server at port " + tcpPort);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
