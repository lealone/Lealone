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
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.master.HMaster;

import com.codefollower.lealone.hbase.server.HBaseTcpServer;

/**
 * 
 * 无法替换org.apache.hadoop.hbase.master.HMaster类，只能使用coprocessor的方式，
 * 在hbase-site.xml中配置hbase.coprocessor.master.classes = com.codefollower.lealone.hbase.engine.HBaseMasterObserver
 *
 */
public class HBaseMasterObserver extends BaseMasterObserver {
    private static HBaseTcpServer server;

    @Override
    public synchronized void start(CoprocessorEnvironment env) throws IOException {
        if (server == null) {
            HMaster m = (HMaster) ((MasterCoprocessorEnvironment) env).getMasterServices();
            server = new HBaseTcpServer(m);
            server.start();
        }
    }

    @Override
    public synchronized void stop(CoprocessorEnvironment env) throws IOException {
        if (server != null) {
            try {
                server.stop();
            } finally {
                server = null;
            }
        }
    }
}
