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
package com.codefollower.yourbase.master;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;

import com.codefollower.yourbase.server.H2TcpServer;

/**
 * 
 * 无法替换org.apache.hadoop.hbase.master.HMaster类，只能使用coprocessor的方式，
 * 在hbase-site.xml中配置hbase.coprocessor.master.classes = com.codefollower.yourbase.coprocessor.HBaseMasterObserver
 *
 */
public class HMaster extends org.apache.hadoop.hbase.master.HMaster {
    private static final Log log = LogFactory.getLog(HMaster.class.getName());
    private H2TcpServer server = new H2TcpServer();

    public HMaster(Configuration conf) throws IOException, KeeperException, InterruptedException {
        super(conf);
    }

    @Override
    public void run() {
        server.start(log, H2TcpServer.getMasterTcpPort(getConfiguration()), this);
        super.run();
        server.stop();
    }

}
