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
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.codefollower.yourbase.constant.Constants;
import com.codefollower.yourbase.message.DbException;

public class TcpServerTracker extends ZooKeeperListener {
    private static final String TCP_SERVER_NODE = "/" + Constants.PROJECT_NAME + "/server";
    private NavigableSet<String> servers = new TreeSet<String>();

    private String masterAddress;

    public TcpServerTracker(ZooKeeperWatcher watcher) {
        super(watcher);
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public void start() throws Exception {
        watcher.registerListener(this);
        List<String> servers = listChildrenAndWatchThem(watcher, TCP_SERVER_NODE);
        add(servers);
    }

    private void add(final List<String> servers) throws IOException {
        synchronized (this.servers) {
            this.servers.clear();
            for (String s : servers) { //s有可能为null，比如当listChildrenAndWatchThem解析到一个Master地址时
                if (s != null)
                    this.servers.add(s);
            }
        }
    }

    private void remove(final String sn) {
        synchronized (this.servers) {
            this.servers.remove(sn);
        }
    }

    @Override
    public void nodeDeleted(String path) {
        if (path.startsWith(TCP_SERVER_NODE)) {
            String serverName = path.substring(path.lastIndexOf("/") + 1);
            serverName = getTcpServerName(serverName);
            if (serverName.startsWith("M:"))
                masterAddress = null;
            else
                remove(serverName.substring(2));
        }
    }

    @Override
    public void nodeChildrenChanged(String path) {
        if (path.equals(TCP_SERVER_NODE)) {
            try {
                List<String> servers = listChildrenAndWatchThem(watcher, TCP_SERVER_NODE);
                add(servers);
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    /**
     * Gets the online servers.
     * @return list of online servers
     */
    public List<String> getOnlineServers() {
        synchronized (servers) {
            return new ArrayList<String>(servers);
        }
    }

    private static String getTcpServerName(String serverName) {
        String prefix = serverName.substring(0, 2);
        serverName = serverName.substring(2);
        return prefix + serverName.substring(0, serverName.indexOf(':')) + serverName.substring(serverName.lastIndexOf(':'));
    }

    private List<String> listChildrenAndWatchThem(ZooKeeperWatcher zkw, String znode) {
        try {
            List<String> children = zkw.getZooKeeper().getChildren(znode, zkw);
            if (children == null) {
                return null;
            }
            for (int i = 0, size = children.size(); i < size; i++) {
                String serverName = getTcpServerName(children.get(i));
                if (serverName.startsWith("M:")) {
                    masterAddress = serverName.substring(2);
                    children.set(i, null);
                } else
                    children.set(i, serverName.substring(2));
            }
            return children;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }
}
