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
package com.codefollower.yourbase.zookeeper;

import java.io.IOException;
import java.util.List;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

public class H2TcpPortTracker extends ZooKeeperListener {

    /*
     * 包含Master他RegionServer的tcp端口
     * key是ServerName.getHostAndPort()
     */
    private ConcurrentHashMap<String, Integer> tcpPortMap = new ConcurrentHashMap<String, Integer>();
    private Abortable abortable;

    private static String getH2TcpPortEphemeralNodePath(ServerName sn, int port) {
        return ZKUtil.joinZNode(ZooKeeperAdmin.TCP_SERVER_NODE, sn.getHostAndPort() + Addressing.HOSTNAME_PORT_SEPARATOR + port);
    }

    public static void createH2TcpPortEphemeralNode(ServerName sn, int port) {
        try {
            ZKUtil.createEphemeralNodeAndWatch(ZooKeeperAdmin.getZooKeeperWatcher(), getH2TcpPortEphemeralNodePath(sn, port),
                    HConstants.EMPTY_BYTE_ARRAY);
        } catch (KeeperException e) {
            throw new H2TcpPortTrackerException(e);
        }
    }

    public static void deleteH2TcpPortEphemeralNode(ServerName sn, int port) {
        try {
            ZKUtil.deleteNode(ZooKeeperAdmin.getZooKeeperWatcher(), getH2TcpPortEphemeralNodePath(sn, port));
        } catch (KeeperException e) {
            throw new H2TcpPortTrackerException(e);
        }
    }

    public H2TcpPortTracker(ZooKeeperWatcher watcher, Abortable abortable) {
        super(watcher);
        this.abortable = abortable;
    }

    public void start() throws KeeperException, IOException {
        watcher.registerListener(this);
        List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, ZooKeeperAdmin.TCP_SERVER_NODE);
        add(servers);
    }

    private void add(final List<String> servers) throws IOException {
        ConcurrentHashMap<String, Integer> tcpPortMap = new ConcurrentHashMap<String, Integer>();
        for (String n : servers) {
            n = ZKUtil.getNodeName(n);
            int pos = n.lastIndexOf(Addressing.HOSTNAME_PORT_SEPARATOR);
            tcpPortMap.put(n.substring(0, pos), Integer.parseInt(n.substring(pos + 1)));
        }

        this.tcpPortMap = tcpPortMap;
    }

    @Override
    public void nodeDeleted(String path) {
        if (path.startsWith(ZooKeeperAdmin.TCP_SERVER_NODE)) {
            String serverName = ZKUtil.getNodeName(path);
            tcpPortMap.remove(serverName.substring(0, serverName.lastIndexOf(Addressing.HOSTNAME_PORT_SEPARATOR)));
        }
    }

    @Override
    public void nodeChildrenChanged(String path) {
        if (path.equals(ZooKeeperAdmin.TCP_SERVER_NODE)) {
            try {
                List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, ZooKeeperAdmin.TCP_SERVER_NODE);
                add(servers);
            } catch (IOException e) {
                abortable.abort("Unexpected zk exception getting server nodes", e);
            } catch (KeeperException e) {
                abortable.abort("Unexpected zk exception getting server nodes", e);
            }
        }
    }

    public int getTcpPort(ServerName sn) {
        return tcpPortMap.get(sn.getHostAndPort());
    }

    public int getTcpPort(HRegionLocation loc) {
        return tcpPortMap.get(loc.getHostnamePort());
    }

    public int getTcpPort(String hostAndPort) {
        return tcpPortMap.get(hostAndPort);
    }
}
