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
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * 
 * 改编自{@link org.apache.hadoop.hbase.zookeeper.RegionServerTracker}
 * 去掉与ServerManager相关的代码
 *
 */
public class RegionServerTracker extends ZooKeeperListener {
    private NavigableSet<ServerName> regionServers = new TreeSet<ServerName>();
    private Abortable abortable;

    public RegionServerTracker(ZooKeeperWatcher watcher, Abortable abortable) {
        super(watcher);
        this.abortable = abortable;
    }

    /**
     * Starts the tracking of online RegionServers.
     *
     * <p>All RSs will be tracked after this method is called.
     *
     * @throws KeeperException
     * @throws IOException
     */
    public void start() throws KeeperException, IOException {
        watcher.registerListener(this);
        List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.rsZNode);
        add(servers);
    }

    private void add(final List<String> servers) throws IOException {
        synchronized (this.regionServers) {
            this.regionServers.clear();
            for (String n : servers) {
                ServerName sn = ServerName.parseServerName(ZKUtil.getNodeName(n));
                this.regionServers.add(sn);
            }
        }
    }

    private void remove(final ServerName sn) {
        synchronized (this.regionServers) {
            this.regionServers.remove(sn);
        }
    }

    @Override
    public void nodeDeleted(String path) {
        if (path.startsWith(watcher.rsZNode)) {
            String serverName = ZKUtil.getNodeName(path);
            ServerName sn = ServerName.parseServerName(serverName);
            remove(sn);
        }
    }

    @Override
    public void nodeChildrenChanged(String path) {
        if (path.equals(watcher.rsZNode)) {
            try {
                List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.rsZNode);
                add(servers);
            } catch (IOException e) {
                abortable.abort("Unexpected zk exception getting RS nodes", e);
            } catch (KeeperException e) {
                abortable.abort("Unexpected zk exception getting RS nodes", e);
            }
        }
    }

    /**
     * Gets the online servers.
     * @return list of online servers
     */
    public List<ServerName> getOnlineServers() {
        synchronized (this.regionServers) {
            return new ArrayList<ServerName>(this.regionServers);
        }
    }
}
