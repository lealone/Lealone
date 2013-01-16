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

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.data.Stat;

import com.codefollower.yourbase.table.H2MetaTable;

public class H2MetaTableTracker extends ZooKeeperListener {
    private static final int NODE_NAME_LENGTH = ZooKeeperAdmin.METATABLE_NODE.length();
    private final H2MetaTable table;
    private final NavigableSet<Integer> dbObjectIDs = new TreeSet<Integer>();
    /**
     * 记录数据库对象(用ID表示)在zookeeper中的最新版本
     */
    private final ConcurrentHashMap<Integer, Integer> idVersionMap = new ConcurrentHashMap<Integer, Integer>();

    public H2MetaTableTracker(ZooKeeperWatcher watcher, H2MetaTable table) {
        super(watcher);
        this.table = table;
    }

    public void start() throws H2MetaTableTrackerException {
        watcher.registerListener(this);
        try {
            List<String> objectIDs = ZKUtil.listChildrenAndWatchThem(watcher, ZooKeeperAdmin.METATABLE_NODE);

            if (objectIDs != null) {
                for (String n : objectIDs)
                    idVersionMap.put(Integer.valueOf(n), 0);
                add(objectIDs, false);
            }
        } catch (Exception e) {
            throw new H2MetaTableTrackerException(e);
        }
    }

    public void refresh() {
        try {
            synchronized (this.dbObjectIDs) {
                List<String> objectIDs = ZKUtil.listChildrenAndWatchThem(watcher, ZooKeeperAdmin.METATABLE_NODE);

                if (objectIDs != null) {
                    for (String n : objectIDs) {
                        Stat stat = new Stat();
                        String node = ZKUtil.joinZNode(ZooKeeperAdmin.METATABLE_NODE, n);
                        ZKUtil.getDataAndWatch(watcher, node, stat);
                        int id = Integer.valueOf(n);
                        Integer version = idVersionMap.get(id);

                        if (version != null) {
                            idVersionMap.put(id, version);
                        }
                        if (version == null) {
                            table.getDatabase().addDatabaseObject(id);
                            idVersionMap.put(id, 0);
                        } else if (stat.getVersion() > version)
                            table.getDatabase().updateDatabaseObject(id);
                    }
                    
                    synchronized (this.dbObjectIDs) {
                        NavigableSet<Integer> oldObjectIDs = new TreeSet<Integer>(this.dbObjectIDs);
                        for (String n : objectIDs) {
                            oldObjectIDs.remove(Integer.valueOf(n));
                        }
                        
                        for (Integer n : oldObjectIDs) {
                            remove(n);
                        }
                    }
                    add(objectIDs, false);
                }
            }
        } catch (Exception e) {
            throw new H2MetaTableTrackerException(e);
        }
    }

    @Override
    public void nodeDeleted(String path) {
        if (path.startsWith(ZooKeeperAdmin.METATABLE_NODE)) {
            int id = Integer.valueOf(path.substring(NODE_NAME_LENGTH + 1));
            remove(id);
        }
    }

    @Override
    public void nodeChildrenChanged(String path) {
        if (path.equals(ZooKeeperAdmin.METATABLE_NODE)) {
            try {
                List<String> objectIDs = ZKUtil.listChildrenAndWatchThem(watcher, ZooKeeperAdmin.METATABLE_NODE);
                add(objectIDs, true);
            } catch (Exception e) {
                throw new H2MetaTableTrackerException(e);
            }
        }
    }

    @Override
    public void nodeCreated(String path) {
        //System.out.println("nodeCreated: path=" + path);
        //此方法很少触发，通常是触发nodeChildrenChanged
    }

    @Override
    public void nodeDataChanged(String path) {
        if (path.length() != NODE_NAME_LENGTH && path.startsWith(ZooKeeperAdmin.METATABLE_NODE)) {
            try {
                synchronized (this.dbObjectIDs) {
                    Stat stat = new Stat();
                    ZKUtil.getDataAndWatch(watcher, path, stat);
                    int id = Integer.valueOf(path.substring(NODE_NAME_LENGTH + 1));
                    Integer version = idVersionMap.get(id);

                    if (version == null)
                        throw new H2MetaTableTrackerException("id: " + id + " not found, it may be a bug");
                    else if (stat.getVersion() > version)
                        table.getDatabase().updateDatabaseObject(id);
                }
            } catch (Exception e) {
                throw new H2MetaTableTrackerException(e);
            }
        }
    }

    private void add(final List<String> newObjectIDs, boolean isNew) throws Exception {
        if (newObjectIDs == null)
            return;
        synchronized (this.dbObjectIDs) {
            NavigableSet<Integer> oldObjectIDs = new TreeSet<Integer>(this.dbObjectIDs);
            this.dbObjectIDs.clear();
            for (String n : newObjectIDs) {
                int id = Integer.valueOf(n);
                if (oldObjectIDs.add(id)) {
                    if (isNew) {
                        idVersionMap.put(Integer.valueOf(n), 0);
                        table.getDatabase().addDatabaseObject(id);
                    }
                }

                this.dbObjectIDs.add(id);
            }
        }
    }

    private void remove(int id) {
        synchronized (dbObjectIDs) {
            if (dbObjectIDs.remove(id)) {
                idVersionMap.remove(id);
                table.getDatabase().removeDatabaseObject(id);
            }
        }
    }
}