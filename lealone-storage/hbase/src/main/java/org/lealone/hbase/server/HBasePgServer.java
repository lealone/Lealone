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

import static org.lealone.hbase.engine.HBaseConstants.DEFAULT_MASTER_PG_PORT;
import static org.lealone.hbase.engine.HBaseConstants.DEFAULT_PG_SERVER_ENABLED;
import static org.lealone.hbase.engine.HBaseConstants.DEFAULT_PG_SERVER_START_ARGS;
import static org.lealone.hbase.engine.HBaseConstants.DEFAULT_REGIONSERVER_PG_PORT;
import static org.lealone.hbase.engine.HBaseConstants.DEFAULT_TABLE_ENGINE;
import static org.lealone.hbase.engine.HBaseConstants.MASTER_PG_PORT;
import static org.lealone.hbase.engine.HBaseConstants.PG_SERVER_ENABLED;
import static org.lealone.hbase.engine.HBaseConstants.PG_SERVER_START_ARGS;
import static org.lealone.hbase.engine.HBaseConstants.REGIONSERVER_PG_PORT;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.lealone.engine.Constants;
import org.lealone.hbase.dbobject.table.HBaseTableEngine;
import org.lealone.hbase.zookeeper.PgPortTracker;
import org.lealone.hbase.zookeeper.ZooKeeperAdmin;
import org.lealone.message.TraceSystem;
import org.lealone.server.PgServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBasePgServer extends PgServer implements Runnable, HBaseServer {
    private static final Logger logger = LoggerFactory.getLogger(HBasePgServer.class);

    public static int getMasterPgPort(Configuration conf) {
        return conf.getInt(MASTER_PG_PORT, DEFAULT_MASTER_PG_PORT);
    }

    public static int getRegionServerPgPort(Configuration conf) {
        return conf.getInt(REGIONSERVER_PG_PORT, DEFAULT_REGIONSERVER_PG_PORT);
    }

    public static boolean isPgServerEnabled(Configuration conf) {
        return conf.getBoolean(PG_SERVER_ENABLED, DEFAULT_PG_SERVER_ENABLED);
    }

    private final int pgPort;
    private final ServerName serverName;

    private HMaster master;
    private HRegionServer regionServer;

    public HBasePgServer(HMaster master) {
        ZooKeeperAdmin.createBaseZNodes();
        initConf(master.getConfiguration());
        pgPort = getMasterPgPort(master.getConfiguration());
        serverName = master.getServerName();
        this.master = master;
        init(master.getConfiguration());
    }

    public HBasePgServer(HRegionServer regionServer) {
        ZooKeeperAdmin.createBaseZNodes();
        initConf(regionServer.getConfiguration());
        pgPort = getRegionServerPgPort(regionServer.getConfiguration());
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
        super.start();
        PgPortTracker.createPgPortEphemeralNode(serverName, pgPort, master != null);
        ZooKeeperAdmin.getPgPortTracker(); //初始化PgPortTracker

        String name = getName() + " (" + getURL() + ")";
        Thread t = new Thread(this, name);
        t.setDaemon(isDaemon());
        t.start();

        logger.info("Lealone PgServer started, listening port: {}", pgPort);
    }

    @Override
    public void stop() {
        try {
            super.stop();
            logger.info("Lealone PgServer stopped");
        } finally {
            PgPortTracker.deletePgPortEphemeralNode(serverName, pgPort, master != null);
        }
    }

    @Override
    public String getName() {
        return "Lealone pg server";
    }

    @Override
    protected HBasePgServerThread createPgServerThread(Socket socket) {
        return new HBasePgServerThread(socket, this);
    }

    private void init(Configuration conf) {
        ArrayList<String> args = new ArrayList<String>();
        for (String arg : conf.getStrings(PG_SERVER_START_ARGS, DEFAULT_PG_SERVER_START_ARGS)) {
            int pos = arg.indexOf('=');
            if (pos == -1) {
                args.add(arg.trim());
            } else {
                args.add(arg.substring(0, pos).trim());
                args.add(arg.substring(pos + 1).trim());
            }
        }
        args.add("-pgPort");
        args.add("" + pgPort);
        super.init(args.toArray(new String[args.size()]));
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
