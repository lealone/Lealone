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
package org.lealone.zookeeper;

import java.util.List;
import java.util.Random;

import org.lealone.message.DbException;

public class ZooKeeperAdmin {

    private static final Random random = new Random(System.currentTimeMillis());

    private final ZooKeeperWatcher watcher;
    private final TcpServerTracker tcpServerTracker;

    public ZooKeeperAdmin(String connectString, int sessionTimeout) {
        watcher = new ZooKeeperWatcher(connectString, sessionTimeout);
        tcpServerTracker = new TcpServerTracker(watcher);
        try {
            tcpServerTracker.start();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public void close() {
        watcher.close();
    }

    /**
     * Gets the online servers.
     * @return list of online servers
     */
    public List<String> getOnlineServers() {
        return tcpServerTracker.getOnlineServers();
    }

    public String getMasterAddress() {
        return tcpServerTracker.getMasterAddress();
    }

    /**
     * 随机获取一个在线的TCP Server
     * 
     * @return 在线的TCP Server
     */
    public String getOnlineServer() {
        List<String> servers = getOnlineServers();
        return servers.get(random.nextInt(servers.size()));
    }
}
