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
package com.codefollower.yourbase.test.start;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;

import com.codefollower.yourbase.hbase.util.HBaseUtils;
import com.codefollower.yourbase.store.fs.FileUtils;

public class HMasterStarter {
    static Configuration conf = HBaseUtils.getConfiguration();

    public static void main(String[] args) throws Exception {
        //删除临时测试目录
        FileUtils.deleteRecursive(conf.get("yourbase.test.dir"), true);

        new ZookeeperThread().start();
        Thread.sleep(1000);
        HMaster.main(new String[] { "start" });
    }

    static class ZookeeperThread extends Thread {
        public void run() {
            MiniZooKeeperCluster zooKeeperCluster = new MiniZooKeeperCluster();

            File zkDataPath = new File(conf.get(HConstants.ZOOKEEPER_DATA_DIR));
            int zkClientPort = conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
            zooKeeperCluster.setDefaultClientPort(zkClientPort);
            try {
                zooKeeperCluster.startup(zkDataPath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
