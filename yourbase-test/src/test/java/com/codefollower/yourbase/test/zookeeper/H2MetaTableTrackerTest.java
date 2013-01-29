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
package com.codefollower.yourbase.test.zookeeper;

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.data.Stat;

import com.codefollower.yourbase.hbase.util.HBaseUtils;
import com.codefollower.yourbase.hbase.zookeeper.MetaTableTrackerException;

public class H2MetaTableTrackerTest implements Abortable {
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception {
        H2MetaTableTrackerTest test1 = new H2MetaTableTrackerTest();
        H2MetaTableTrackerTest test2 = new H2MetaTableTrackerTest();
        H2MetaTableTrackerTest test3 = new H2MetaTableTrackerTest(false);

        ZKUtil.setData(test3.watcher, H2MetaTableTracker.NODE_NAME, Bytes.toBytes(System.currentTimeMillis()));
        Thread.sleep(2000);
    }

    public static void main1(String[] args) throws Exception {
        H2MetaTableTrackerTest test = new H2MetaTableTrackerTest();

        String node = ZKUtil.joinZNode(H2MetaTableTracker.NODE_NAME, Integer.toString(120));
        ZKUtil.deleteNodeRecursively(test.watcher, node);
        //ZKUtil.deleteNodeFailSilent(test.watcher, node);
        ZKUtil.createAndWatch(test.watcher, node, EMPTY_BYTE_ARRAY);
        ZKUtil.createAndWatch(test.watcher, node + "/ddd", EMPTY_BYTE_ARRAY);

        node = ZKUtil.joinZNode(H2MetaTableTracker.NODE_NAME, Integer.toString(130));
        ZKUtil.deleteNodeFailSilent(test.watcher, node);
        ZKUtil.createAndWatch(test.watcher, node, EMPTY_BYTE_ARRAY);

        node = ZKUtil.joinZNode(H2MetaTableTracker.NODE_NAME, Integer.toString(140));
        ZKUtil.deleteNodeFailSilent(test.watcher, node);
        ZKUtil.createAndWatch(test.watcher, node, EMPTY_BYTE_ARRAY);

        long ts = System.currentTimeMillis();
        ZKUtil.getDataNoWatch(test.watcher, node, null); //即使这里设成NoWatch了，setData时还是会触发nodeDataChanged
        ZKUtil.getData(test.watcher, node);
        Stat stat = new Stat();
        ZKUtil.getDataAndWatch(test.watcher, node, stat);
        System.out.println("stat.getVersion()=" + stat.getVersion());
        ZKUtil.setData(test.watcher, node, Bytes.toBytes(ts));
        //ZKUtil.watchAndCheckExists(test.watcher, node);

        stat = new Stat();
        ZKUtil.getDataAndWatch(test.watcher, node, stat);
        System.out.println("stat.getVersion()=" + stat.getVersion());

        //ts = System.currentTimeMillis()+10;
        ZKUtil.setData(test.watcher, node, Bytes.toBytes(ts));

        stat = new Stat();
        ZKUtil.getDataAndWatch(test.watcher, node, stat);
        System.out.println("stat.getVersion()=" + stat.getVersion());

        node = ZKUtil.joinZNode(H2MetaTableTracker.NODE_NAME, Integer.toString(150));
        ZKUtil.createEphemeralNodeAndWatch(test.watcher, node, EMPTY_BYTE_ARRAY);

    }

    private final ZooKeeperWatcher watcher;
    private final H2MetaTableTracker tracker;

    public H2MetaTableTrackerTest() throws Exception {
        watcher = new ZooKeeperWatcher(HBaseUtils.getConfiguration(), "H2MetaTableTrackerTest", this);
        tracker = new H2MetaTableTracker(watcher);
        tracker.start();
    }

    public H2MetaTableTrackerTest(boolean start) throws Exception {
        watcher = new ZooKeeperWatcher(HBaseUtils.getConfiguration(), "H2MetaTableTrackerTest", this);
        tracker = new H2MetaTableTracker(watcher);
        if (start)
            tracker.start();
    }

    @Override
    public void abort(String why, Throwable e) {

    }

    @Override
    public boolean isAborted() {

        return false;
    }

    public static class H2MetaTableTracker extends ZooKeeperListener {
        public static final String NODE_NAME = "/yourbase/metatable2";
        private static final int NODE_NAME_LENGTH = NODE_NAME.length();
        private final NavigableSet<Integer> objectIDs = new TreeSet<Integer>();

        public H2MetaTableTracker(ZooKeeperWatcher watcher) {
            super(watcher);
        }

        public void start() throws MetaTableTrackerException {
            watcher.registerListener(this);
            try {
                ZKUtil.createAndFailSilent(watcher, "/yourbase");
                ZKUtil.createAndFailSilent(watcher, NODE_NAME);

                ZKUtil.watchAndCheckExists(watcher, NODE_NAME);

                List<String> objectIDs = ZKUtil.listChildrenAndWatchThem(watcher, NODE_NAME);
                System.out.println("start: objectIDs=" + objectIDs);
                add(objectIDs, false);
            } catch (Exception e) {
                throw new MetaTableTrackerException(e);
            }
        }

        public boolean contains(int id) {
            return objectIDs.contains(id);
        }

        public boolean addId(int id) {
            return objectIDs.add(id);
        }

        private void add(final List<String> newObjectIDs, boolean executeMetaRecord) throws Exception {
            if (newObjectIDs == null)
                return;
            synchronized (this.objectIDs) {
                NavigableSet<Integer> oldObjectIDs = new TreeSet<Integer>(this.objectIDs);
                this.objectIDs.clear();
                for (String n : newObjectIDs) {
                    int id = Integer.valueOf(n);
                    if (oldObjectIDs.add(id)) {
                    }

                    this.objectIDs.add(id);
                }
            }
        }

        private void remove(int id) {
            synchronized (objectIDs) {
                if (objectIDs.remove(id)) {
                }
            }
        }

        @Override
        public void nodeDeleted(String path) {
            System.out.println("nodeDeleted: path=" + path);
            if (path.startsWith(NODE_NAME)) {
                int id = Integer.valueOf(path.substring(NODE_NAME_LENGTH + 1));
                remove(id);
            }
        }

        @Override
        public void nodeChildrenChanged(String path) {
            System.out.println("nodeChildrenChanged: path=" + path);
            if (path.equals(NODE_NAME)) {
                try {
                    List<String> objectIDs = ZKUtil.listChildrenAndWatchThem(watcher, NODE_NAME);
                    System.out.println("objectIDs=" + objectIDs);
                    add(objectIDs, true);
                } catch (Exception e) {
                    throw new MetaTableTrackerException(e);
                }
            }
        }

        @Override
        public void nodeCreated(String path) {
            System.out.println("nodeCreated: path=" + path);
            //        if (path.equals(NODE_NAME)) {
            //            try {
            //                List<String> objectIDs = ZKUtil.listChildrenAndWatchThem(watcher, NODE_NAME);
            //                add(objectIDs);
            //            } catch (Exception e) {
            //                throw new H2MetaTableTrackerException(e);
            //            }
            //        }
        }

        @Override
        public void nodeDataChanged(String path) {
            System.out.println("nodeDataChanged: path=" + path);
            if (path.length() != NODE_NAME_LENGTH && path.startsWith(NODE_NAME)) {
                try {
                    //int id = Integer.valueOf(path.substring(NODE_NAME_LENGTH + 1));
                } catch (Exception e) {
                    throw new MetaTableTrackerException(e);
                }
            }
        }
    }
}
