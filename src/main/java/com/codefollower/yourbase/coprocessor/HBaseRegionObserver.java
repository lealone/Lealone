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
package com.codefollower.yourbase.coprocessor;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import com.codefollower.yourbase.server.H2TcpServer;

/**
 * 
 * 无法使用coprocessor的方式，
 * 只能在hbase-site.xml中配置hbase.regionserver.impl = com.codefollower.yourbase.regionserver.HRegionServer
 *
 */
public class HBaseRegionObserver extends BaseRegionObserver {
    private static final Log log = LogFactory.getLog(HBaseRegionObserver.class.getName());
    private static H2TcpServer server;

    @Override
    public synchronized void start(CoprocessorEnvironment env) throws IOException {
        if (server == null) {
            server = new H2TcpServer();
            HRegionServer rs = (HRegionServer) ((RegionCoprocessorEnvironment) env).getRegionServerServices();
            server.start(log, H2TcpServer.getRegionServerTcpPort(rs.getConfiguration()), rs);
        }
    }

    @Override
    public synchronized void stop(CoprocessorEnvironment env) throws IOException {
        //BaseRegionObserver是针对单个Region的，并不是针对整个RegionServer，没有专门的针对RegionServer的Observer，
        //这个stop方法有可能调用多次，所以不能关闭server
        //        if (server != null) {
        //            try {
        //                server.stop();
        //            } finally {
        //                server = null;
        //            }
        //        }
    }
}
