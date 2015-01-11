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
package org.lealone.hbase.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.lealone.message.DbException;

public class PgPortTracker extends ZooKeeperListener {

    /*
     * 包含Master他RegionServer的pg端口
     * key是ServerName.getHostAndPort()
     */
    private ConcurrentHashMap<String, Integer> pgPortMap = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, String> pgListenAddressMap = new ConcurrentHashMap<String, String>();
    private final Abortable abortable;

    private static String getPgPortEphemeralNodePath(ServerName sn, String listenAddress, int port, boolean isMaster) {
        String znode = (isMaster ? "M" : "S") + ":" + sn.getHostAndPort() + ":" + listenAddress + ":" + port;
        return ZKUtil.joinZNode(ZooKeeperAdmin.PG_SERVER_NODE, znode);
    }

    public static void createPgPortEphemeralNode(ServerName sn, String listenAddress, int port, boolean isMaster) {
        try {
            ZKUtil.createEphemeralNodeAndWatch(ZooKeeperAdmin.getZooKeeperWatcher(),
                    getPgPortEphemeralNodePath(sn, listenAddress, port, isMaster), HConstants.EMPTY_BYTE_ARRAY);
        } catch (KeeperException e) {
            throw DbException.convert(e);
        }
    }

    public static void deletePgPortEphemeralNode(ServerName sn, String listenAddress, int port, boolean isMaster) {
        try {
            ZKUtil.deleteNode(ZooKeeperAdmin.getZooKeeperWatcher(), //
                    getPgPortEphemeralNodePath(sn, listenAddress, port, isMaster));
        } catch (KeeperException e) {
            throw DbException.convert(e);
        }
    }

    public PgPortTracker(ZooKeeperWatcher watcher, Abortable abortable) {
        super(watcher);
        this.abortable = abortable;
    }

    public void start() throws KeeperException, IOException {
        watcher.registerListener(this);
        List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, ZooKeeperAdmin.PG_SERVER_NODE);
        add(servers);
    }

    private void add(final List<String> servers) throws IOException {
        ConcurrentHashMap<String, Integer> pgPortMap = new ConcurrentHashMap<String, Integer>();
        ConcurrentHashMap<String, String> pgListenAddressMap = new ConcurrentHashMap<String, String>();
        String serverName;
        String[] a;
        for (String n : servers) {
            a = n.split(":");
            serverName = a[1] + ":" + a[2];

            pgPortMap.put(serverName, Integer.parseInt(a[4]));
            pgListenAddressMap.put(serverName, a[3]);
        }

        this.pgPortMap = pgPortMap;
        this.pgListenAddressMap = pgListenAddressMap;
    }

    @Override
    public void nodeDeleted(String path) {
        if (path.startsWith(ZooKeeperAdmin.PG_SERVER_NODE)) {
            String serverName = ZKUtil.getNodeName(path);
            String[] a = serverName.split(":");
            serverName = a[1] + ":" + a[2];
            pgPortMap.remove(serverName);
            pgListenAddressMap.remove(serverName);
        }
    }

    @Override
    public void nodeChildrenChanged(String path) {
        if (path.equals(ZooKeeperAdmin.PG_SERVER_NODE)) {
            try {
                List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, ZooKeeperAdmin.PG_SERVER_NODE);
                add(servers);
            } catch (IOException e) {
                abortable.abort("Unexpected zk exception getting server nodes", e);
            } catch (KeeperException e) {
                abortable.abort("Unexpected zk exception getting server nodes", e);
            }
        }
    }

    @Override
    public void nodeCreated(String path) {
        nodeChildrenChanged(path);
    }

    @Override
    public void nodeDataChanged(String path) {
        nodeChildrenChanged(path);
    }

    public int getPgPort(ServerName sn) {
        return pgPortMap.get(sn.getHostAndPort());
    }

    public int getPgPort(HRegionLocation loc) {
        return pgPortMap.get(loc.getHostnamePort());
    }

    public int getPgPort(String hostAndPort) {
        return pgPortMap.get(hostAndPort);
    }

    public String getPgListenAddress(ServerName sn) {
        return pgListenAddressMap.get(sn.getHostAndPort());
    }
}
