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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegionServerCommandLine;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.util.ToolRunner;

//共用同一个hbase-site文件，但是使用三个不同的端口号
public class AnotherHRegionServerStarter {

    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            args = new String[] { "start" };

        Configuration conf = HBaseConfiguration.create();
        @SuppressWarnings("unchecked")
        Class<? extends HRegionServer> regionServerClass = (Class<? extends HRegionServer>) conf.getClass(
                HConstants.REGION_SERVER_IMPL, HRegionServer.class);
        int hbaseRegionServerPort = 60020;
        int hbaseRegionServerInfoPort = 60030;
        int yourbaseRegionServerTcpPort = 9092;

        //        new Thread(new MyHRegionServerCommandLine(regionServerClass, conf, args, hbaseRegionServerPort,
        //                hbaseRegionServerInfoPort, yourbaseRegionServerTcpPort)).start();

        hbaseRegionServerPort = 60021;
        hbaseRegionServerInfoPort = 60031;
        yourbaseRegionServerTcpPort = 9093;

        new Thread(new MyHRegionServerCommandLine(regionServerClass, conf, args, hbaseRegionServerPort,
                hbaseRegionServerInfoPort, yourbaseRegionServerTcpPort)).start();
    }

    static class MyHRegionServerCommandLine extends HRegionServerCommandLine implements Runnable {

        Configuration conf;
        String args[];

        public MyHRegionServerCommandLine(Class<? extends HRegionServer> clazz, Configuration conf, String args[],
                int hbaseRegionServerPort, int hbaseRegionServerInfoPort, int yourbaseRegionServerTcpPort) {
            super(clazz);
            this.conf = new Configuration(conf);
            this.args = args;
            this.conf.setInt("hbase.regionserver.port", hbaseRegionServerPort);
            this.conf.setInt("hbase.regionserver.info.port", hbaseRegionServerInfoPort);
            this.conf.setInt("yourbase.regionserver.tcp.port", yourbaseRegionServerTcpPort);
        }

        @Override
        public void run() {
            try {
                VersionInfo.logVersion();

                int ret = ToolRunner.run(conf, this, args);
                if (ret != 0) {
                    System.exit(ret);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
