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
package com.codefollower.lealone.test.start;

import java.util.List;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class BookieServerStarter {
    private static String testDir = HBaseConfiguration.create().get("lealone.test.dir");

    public static void main(String[] args) throws Exception {
        //建立必须的bookkeeper节点
        ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 12000, new MyWatcher());
        deleteNodeRecursivelyAndFailSilent(zk, "/ledgers");

        createAndFailSilent(zk, "/ledgers");
        createAndFailSilent(zk, "/ledgers/cookies");
        createAndFailSilent(zk, "/ledgers/available");
        zk.close();

        //删除临时测试目录
        //FileUtils.deleteRecursive(testDir + "/bookkeeper", true);

        String journalDirectory = testDir + "/bookkeeper/journalDirectory";
        String ledgerDirectories = testDir + "/bookkeeper/ledgerDirectories";

        //<bookie_port> <zk_servers> <journal_dir> <ledger_dir [ledger_dir]>
        String newArgs[] = { "3181", "127.0.0.1:2181", journalDirectory, ledgerDirectories };

        BookieServer.main(newArgs);
    }

    private static class MyWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
        }
    }

    private static void createAndFailSilent(ZooKeeper zk, String znode) {
        try {
            //zk.delete(znode, -1);
            zk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {
            //e.printStackTrace();
        }
    }

    private static void deleteNodeRecursivelyAndFailSilent(ZooKeeper zk, String znode) throws Exception {
        try {
            List<String> children = zk.getChildren(znode, false);
            if (children == null)
                return;

            if (!children.isEmpty()) {
                for (String child : children) {
                    deleteNodeRecursivelyAndFailSilent(zk, znode + "/" + child);
                }
            }
            zk.delete(znode, -1);
        } catch (Exception e) {
            //e.printStackTrace();
        }
    }
}
