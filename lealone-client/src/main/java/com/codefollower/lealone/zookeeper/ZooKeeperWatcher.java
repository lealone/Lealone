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
package com.codefollower.lealone.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.codefollower.lealone.message.DbException;

public class ZooKeeperWatcher implements Watcher {
    // listeners to be notified
    private final List<ZooKeeperListener> listeners = new CopyOnWriteArrayList<ZooKeeperListener>();
    private final ZooKeeper zk;

    public ZooKeeperWatcher(String connectString, int sessionTimeout) {
        try {
            this.zk = new ZooKeeper(connectString, sessionTimeout, this);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    public ZooKeeper getZooKeeper() {
        return zk;
    }

    public void close() {
        try {
            zk.close();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Register the specified listener to receive ZooKeeper events.
     * @param listener
     */
    public void registerListener(ZooKeeperListener listener) {
        listeners.add(listener);
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
        case None: {
            connectionEvent(event);
            break;
        }

        case NodeCreated: {
            for (ZooKeeperListener listener : listeners) {
                listener.nodeCreated(event.getPath());
            }
            break;
        }

        case NodeDeleted: {
            for (ZooKeeperListener listener : listeners) {
                listener.nodeDeleted(event.getPath());
            }
            break;
        }

        case NodeDataChanged: {
            for (ZooKeeperListener listener : listeners) {
                listener.nodeDataChanged(event.getPath());
            }
            break;
        }

        case NodeChildrenChanged: {
            for (ZooKeeperListener listener : listeners) {
                listener.nodeChildrenChanged(event.getPath());
            }
            break;
        }
        }
    }

    private void connectionEvent(WatchedEvent event) {
        switch (event.getState()) {
        case SyncConnected:
        case SaslAuthenticated:
        case AuthFailed:
        case Disconnected:
            break;
        case Expired:
            throw DbException.convert(new KeeperException.SessionExpiredException());
        }
    }
}
