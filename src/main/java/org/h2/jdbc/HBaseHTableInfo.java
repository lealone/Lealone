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
package org.h2.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;
import org.h2.util.HBaseUtils;

//TODO 目前未使用
class HBaseHTableInfo {
    HBaseJdbcConnection conn;
    Configuration conf;
    byte[] tableName;
    byte[] start;
    byte[] end;

    private ThreadPoolExecutor pool;
    //如果是false，那么按org.apache.hadoop.hbase.client.ClientScanner的功能来实现
    //只要Select语句中出现聚合函数、groupBy、Having三者之一都被认为是GroupQuery。
    //对于GroupQuery需要把Select语句同时发给相关的RegionServer，得到结果后再在client一起合并。
    boolean isGroupQuery = false;

    List<JdbcConnection> getJdbcConnections() {
        List<byte[]> startKeys;
        try {
            if (isGroupQuery) {
                this.pool = new ThreadPoolExecutor(1, 20, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                        Threads.newDaemonThreadFactory("HBaseJdbcGroupQueryCommand"));
                ((ThreadPoolExecutor) this.pool).allowCoreThreadTimeOut(true);
                startKeys = HBaseUtils.getStartKeysInRange(tableName, start, end);
            } else {
                startKeys = new ArrayList<byte[]>(1);
                startKeys.add(start);
            }
            List<JdbcConnection> conns = new ArrayList<JdbcConnection>();
            if (startKeys != null && startKeys.size() > 0) {
                for (byte[] startKey : startKeys) {
                    conns.add(conn.newJdbcConnection(tableName, startKey));
                }
            }
            return conns;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
