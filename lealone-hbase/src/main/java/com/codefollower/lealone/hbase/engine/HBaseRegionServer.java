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
package com.codefollower.lealone.hbase.engine;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;

import com.codefollower.lealone.hbase.server.HBasePgServer;
import com.codefollower.lealone.hbase.server.HBaseTcpServer;
import com.codefollower.lealone.hbase.transaction.TimestampService;

/**
 * 
 * 在hbase-site.xml中配置hbase.regionserver.impl = com.codefollower.lealone.hbase.engine.HBaseRegionServer
 *
 */
public class HBaseRegionServer extends org.apache.hadoop.hbase.regionserver.HRegionServer {
    private TimestampService timestampService;

    public HBaseRegionServer(Configuration conf) throws IOException, InterruptedException {
        super(conf);
        String classes = conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, null);
        if (classes == null)
            classes = RegionLocationCacheObserver.class.getName();
        else
            classes += "," + RegionLocationCacheObserver.class.getName();
        conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, classes);
    }

    public TimestampService getTimestampService() {
        if (timestampService == null)
            timestampService = new TimestampService(getServerName().getHostAndPort());
        return timestampService;
    }

    @Override
    public void run() {
        HBaseTcpServer server = new HBaseTcpServer(this);
        server.start();

        HBasePgServer pgServer = null;
        if (HBasePgServer.isPgServerEnabled(getConfiguration())) {
            pgServer = new HBasePgServer(this);
            pgServer.start();
        }

        try {
            super.run();
        } finally {
            server.stop();
            if (pgServer != null)
                pgServer.stop();
        }
    }
}
