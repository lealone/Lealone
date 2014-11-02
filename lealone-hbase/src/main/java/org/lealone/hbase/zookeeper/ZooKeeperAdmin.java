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
package org.lealone.hbase.zookeeper;

import java.util.List;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterAddressTracker;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.lealone.constant.Constants;
import org.lealone.hbase.util.HBaseUtils;

public class ZooKeeperAdmin {
    public static final String LEALONE_NODE = "/" + Constants.PROJECT_NAME;
    public static final String DDL_REDO_TABLE_NODE = ZKUtil.joinZNode(LEALONE_NODE, "ddl_redo_table");
    public static final String TCP_SERVER_NODE = ZKUtil.joinZNode(LEALONE_NODE, Constants.TCP_SERVER_NAME);
    public static final String PG_SERVER_NODE = ZKUtil.joinZNode(LEALONE_NODE, "pg_server");
    public static final String REGION_LOCATION_CACHE_NODE = ZKUtil.joinZNode(LEALONE_NODE, "region_location_cache");

    private static Abortable abortable = newAbortable();
    private static ZooKeeperWatcher watcher;
    private static MasterAddressTracker masterAddressTracker;
    private static RegionServerTracker regionServerTracker;
    private static TcpPortTracker tcpPortTracker;
    private static PgPortTracker pgPortTracker;

    private static void reset() {
        watcher = null;
        masterAddressTracker = null;
        regionServerTracker = null;
        tcpPortTracker = null;
        pgPortTracker = null;
        abortable = newAbortable();
    }

    public static Abortable newAbortable() {
        return new Abortable() {
            private boolean abort = false;

            //TODO
            //像org.apache.hadoop.hbase.master.HMaster.abort(String, Throwable)那样
            //处理KeeperException.SessionExpiredException
            @Override
            public void abort(String why, Throwable e) {
                abort = true;
                reset();
            }

            @Override
            public boolean isAborted() {
                return abort;
            }
        };
    }

    public static void createBaseZNodes() {
        try {
            ZKUtil.createAndFailSilent(getZooKeeperWatcher(), LEALONE_NODE);
            ZKUtil.createAndFailSilent(getZooKeeperWatcher(), DDL_REDO_TABLE_NODE);
            ZKUtil.createAndFailSilent(getZooKeeperWatcher(), TCP_SERVER_NODE);
            ZKUtil.createAndFailSilent(getZooKeeperWatcher(), PG_SERVER_NODE);
            ZKUtil.createAndFailSilent(getZooKeeperWatcher(), REGION_LOCATION_CACHE_NODE);
        } catch (Exception e) {
            throw new ZooKeeperAdminException("createBaseZNodes()", e);
        }
    }

    public static ServerName getMasterAddress() {
        return getMasterAddressTracker().getMasterAddress();
    }

    public static MasterAddressTracker getMasterAddressTracker() {
        if (masterAddressTracker == null) {
            synchronized (ZooKeeperAdmin.class) {
                if (masterAddressTracker == null) {
                    masterAddressTracker = new MasterAddressTracker(getZooKeeperWatcher(), abortable);
                    masterAddressTracker.start();
                    try {
                        blockUntilAvailable(masterAddressTracker);
                    } catch (Exception e) {
                        if (e instanceof ZooKeeperAdminException)
                            throw (ZooKeeperAdminException) e;
                        else
                            throw new ZooKeeperAdminException("getMasterAddressTracker()", e);
                    }
                }
            }
        }
        return masterAddressTracker;
    }

    public static List<ServerName> getOnlineServers() {
        return getRegionServerTracker().getOnlineServers();
    }

    public static RegionServerTracker getRegionServerTracker() {
        if (regionServerTracker == null) {
            synchronized (ZooKeeperAdmin.class) {
                if (regionServerTracker == null) {
                    regionServerTracker = new RegionServerTracker(getZooKeeperWatcher(), abortable);
                    try {
                        regionServerTracker.start();
                    } catch (Exception e) {
                        throw new ZooKeeperAdminException("getRegionServerTracker()", e);
                    }
                }
            }
        }
        return regionServerTracker;
    }

    public static int getTcpPort(ServerName sn) {
        return getTcpPortTracker().getTcpPort(sn);
    }

    public static int getTcpPort(HRegionLocation loc) {
        return getTcpPortTracker().getTcpPort(loc);
    }

    public static int getTcpPort(String hostAndPort) {
        return getTcpPortTracker().getTcpPort(hostAndPort);
    }

    public static TcpPortTracker getTcpPortTracker() {
        if (tcpPortTracker == null) {
            synchronized (ZooKeeperAdmin.class) {
                if (tcpPortTracker == null) {
                    tcpPortTracker = new TcpPortTracker(getZooKeeperWatcher(), abortable);
                    try {
                        tcpPortTracker.start();
                    } catch (Exception e) {
                        throw new ZooKeeperAdminException("getTcpPortTracker()", e);
                    }
                }
            }
        }
        return tcpPortTracker;
    }

    public static PgPortTracker getPgPortTracker() {
        if (pgPortTracker == null) {
            synchronized (ZooKeeperAdmin.class) {
                if (pgPortTracker == null) {
                    pgPortTracker = new PgPortTracker(getZooKeeperWatcher(), abortable);
                    try {
                        pgPortTracker.start();
                    } catch (Exception e) {
                        throw new ZooKeeperAdminException("getPgPortTracker()", e);
                    }
                }
            }
        }
        return pgPortTracker;
    }

    public static ZooKeeperWatcher getZooKeeperWatcher() {
        if (watcher == null) {
            synchronized (ZooKeeperAdmin.class) {
                if (watcher == null) {
                    try {
                        watcher = new ZooKeeperWatcher(HBaseUtils.getConfiguration(), "ZooKeeperAdmin", abortable);
                    } catch (Exception e) {
                        throw new ZooKeeperAdminException("getZooKeeperWatcher()", e);
                    }
                }
            }
        }
        return watcher;
    }

    public static void closeZooKeeperWatcher() {
        if (watcher != null) {
            watcher.close();
            watcher = null;
        }
    }

    private static void blockUntilAvailable(ZooKeeperNodeTracker tracker) throws Exception {
        if (tracker.blockUntilAvailable(5000, false) == null) {
            throw new ZooKeeperAdminException("node: " + tracker.getNode() + " no data");
        }
    }
}
