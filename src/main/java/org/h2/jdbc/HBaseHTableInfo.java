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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

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
                startKeys = getStartKeysInRange();
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

    //-----------------以下代码来自org.apache.hadoop.hbase.client.HTable---------------------------//

    private List<byte[]> getStartKeysInRange() throws IOException {
        Pair<byte[][], byte[][]> startEndKeys = getStartEndKeys();
        byte[][] startKeys = startEndKeys.getFirst();
        byte[][] endKeys = startEndKeys.getSecond();

        if (start == null) {
            start = HConstants.EMPTY_START_ROW;
        }
        if (end == null) {
            end = HConstants.EMPTY_END_ROW;
        }

        List<byte[]> rangeKeys = new ArrayList<byte[]>();
        for (int i = 0; i < startKeys.length; i++) {
            if (Bytes.compareTo(start, startKeys[i]) >= 0) {
                if (Bytes.equals(endKeys[i], HConstants.EMPTY_END_ROW) || Bytes.compareTo(start, endKeys[i]) < 0) {
                    rangeKeys.add(start);
                }
            } else if (Bytes.equals(end, HConstants.EMPTY_END_ROW) || Bytes.compareTo(startKeys[i], end) <= 0) {
                rangeKeys.add(startKeys[i]);
            } else {
                break; // past stop
            }
        }

        return rangeKeys;
    }

    /**
     * Gets the starting and ending row keys for every region in the currently
     * open table.
     * <p>
     * This is mainly useful for the MapReduce integration.
     * @return Pair of arrays of region starting and ending row keys
     * @throws IOException if a remote or network exception occurs
     */
    private Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
        NavigableMap<HRegionInfo, ServerName> regions = getRegionLocations();
        final List<byte[]> startKeyList = new ArrayList<byte[]>(regions.size());
        final List<byte[]> endKeyList = new ArrayList<byte[]>(regions.size());

        for (HRegionInfo region : regions.keySet()) {
            startKeyList.add(region.getStartKey());
            endKeyList.add(region.getEndKey());
        }

        return new Pair<byte[][], byte[][]>(startKeyList.toArray(new byte[startKeyList.size()][]),
                endKeyList.toArray(new byte[endKeyList.size()][]));
    }

    /**
     * Gets all the regions and their address for this table.
     * <p>
     * This is mainly useful for the MapReduce integration.
     * @return A map of HRegionInfo with it's server address
     * @throws IOException if a remote or network exception occurs
     */
    private NavigableMap<HRegionInfo, ServerName> getRegionLocations() throws IOException {
        return MetaScanner.allTableRegions(conf, tableName, false);
    }

}
