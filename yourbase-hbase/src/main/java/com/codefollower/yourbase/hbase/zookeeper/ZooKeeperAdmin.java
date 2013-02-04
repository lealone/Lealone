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
package com.codefollower.yourbase.hbase.zookeeper;

import java.util.List;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterAddressTracker;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import com.codefollower.yourbase.constant.Constants;
import com.codefollower.yourbase.hbase.util.HBaseUtils;

public class ZooKeeperAdmin {
    public static final String YOURBASE_NODE = "/" + Constants.PROJECT_NAME;
    public static final String METATABLE_NODE = ZKUtil.joinZNode(YOURBASE_NODE, "metatable");
    public static final String TCP_SERVER_NODE = ZKUtil.joinZNode(YOURBASE_NODE, "server");

    private static Abortable abortable = newAbortable();
    private static ZooKeeperWatcher watcher;
    private static MasterAddressTracker masterAddressTracker;
    private static RegionServerTracker regionServerTracker;
    private static TcpPortTracker h2TcpPortTracker;

    private static void reset() {
        watcher = null;
        masterAddressTracker = null;
        regionServerTracker = null;
        h2TcpPortTracker = null;
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
            ZKUtil.createAndFailSilent(getZooKeeperWatcher(), YOURBASE_NODE);
            ZKUtil.createAndFailSilent(getZooKeeperWatcher(), METATABLE_NODE);
            ZKUtil.createAndFailSilent(getZooKeeperWatcher(), TCP_SERVER_NODE);
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
        return getH2TcpPortTracker().getTcpPort(sn);
    }

    public static int getTcpPort(HRegionLocation loc) {
        return getH2TcpPortTracker().getTcpPort(loc);
    }

    public static int getTcpPort(String hostAndPort) {
        return getH2TcpPortTracker().getTcpPort(hostAndPort);
    }

    public static TcpPortTracker getH2TcpPortTracker() {
        if (h2TcpPortTracker == null) {
            synchronized (ZooKeeperAdmin.class) {
                if (h2TcpPortTracker == null) {
                    h2TcpPortTracker = new TcpPortTracker(getZooKeeperWatcher(), abortable);
                    try {
                        h2TcpPortTracker.start();
                    } catch (Exception e) {
                        throw new ZooKeeperAdminException("getH2TcpPortTracker()", e);
                    }
                }
            }
        }
        return h2TcpPortTracker;
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
