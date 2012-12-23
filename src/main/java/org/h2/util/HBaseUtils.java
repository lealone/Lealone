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
package org.h2.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class HBaseUtils {
    private static final Configuration conf = HBaseConfiguration.create();
    private static HConnection hConnection;

    private HBaseUtils() {
        // utility class
    }

    public static Configuration getConfiguration() {
        return conf;
    }

    public static byte[] toBytes(String s) {
        return Bytes.toBytes(s);
    }

    public static String toString(byte[] b) {
        return Bytes.toString(b);
    }

    public static String createURL(HRegionLocation regionLocation) {
        return createURL(regionLocation.getHostname(), regionLocation.getH2TcpPort());
    }

    public static String createURL(String hostname, int port) {
        // String url = "jdbc:h2:tcp://" + regionLocation.getHostname() + ":" +
        // "regionLocation.getH2TcpPort() + "/hbasedb";//;disableCheck=true
        StringBuilder url = new StringBuilder(50);
        url.append("jdbc:h2:tcp://").append(hostname).append(":").append(port).append("/hbasedb;STORE_ENGINE_NAME=HBASE");
        return url.toString();
    }

    public static HConnection getConnection() throws IOException {
        if (hConnection == null || hConnection.isClosed()) {
            synchronized (HBaseUtils.class) {
                if (hConnection == null || hConnection.isClosed())
                    hConnection = HConnectionManager.createConnection(conf);
            }
        }
        return hConnection;
    }

    public static String getMasterURL() throws IOException {
        ServerName sn = getConnection().getMasterAddress();
        return createURL(sn.getHostname(), sn.getH2TcpPort());
    }

    public static String getRegionServerURL(String tableName, String rowKey) throws IOException {
        return getRegionServerURL(Bytes.toBytes(tableName), Bytes.toBytes(rowKey));
    }

    public static String getRegionServerURL(byte[] tableName, byte[] rowKey) throws IOException {
        HRegionLocation regionLocation = getConnection().locateRegion(tableName, rowKey);
        return createURL(regionLocation);
    }

    public static HBaseRegionInfo getHBaseRegionInfo(String tableName, String rowKey) throws IOException {
        return getHBaseRegionInfo(Bytes.toBytes(tableName), Bytes.toBytes(rowKey));
    }

    public static HBaseRegionInfo getHBaseRegionInfo(byte[] tableName, byte[] rowKey) throws IOException {
        HRegionLocation regionLocation = getConnection().locateRegion(tableName, rowKey);
        return new HBaseRegionInfo(regionLocation);
        //return new HBaseRegionInfo(regionLocation.getRegionInfo().getRegionNameAsString(), createURL(regionLocation));
    }

    //-----------------以下代码来自org.apache.hadoop.hbase.client.HTable---------------------------//

    public static List<byte[]> getStartKeysInRange(byte[] tableName, byte[] startKey, byte[] endKey) throws IOException {
        Pair<byte[][], byte[][]> startEndKeys = getStartEndKeys(tableName);
        byte[][] startKeys = startEndKeys.getFirst();
        byte[][] endKeys = startEndKeys.getSecond();

        if (startKey == null) {
            startKey = HConstants.EMPTY_START_ROW;
        }
        if (endKey == null) {
            endKey = HConstants.EMPTY_END_ROW;
        }

        List<byte[]> rangeKeys = new ArrayList<byte[]>();
        for (int i = 0; i < startKeys.length; i++) {
            if (Bytes.compareTo(startKey, startKeys[i]) >= 0) {
                if (Bytes.equals(endKeys[i], HConstants.EMPTY_END_ROW) || Bytes.compareTo(startKey, endKeys[i]) < 0) {
                    rangeKeys.add(startKey);
                }
            } else if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) || //
                    Bytes.compareTo(startKeys[i], endKey) < 0) { //原先代码是<=，因为coprocessorExec的语义是要包含endKey的
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
    public static Pair<byte[][], byte[][]> getStartEndKeys(byte[] tableName) throws IOException {
        NavigableMap<HRegionInfo, ServerName> regions = getRegionLocations(tableName);
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
    public static NavigableMap<HRegionInfo, ServerName> getRegionLocations(byte[] tableName) throws IOException {
        return MetaScanner.allTableRegions(conf, tableName, false);
    }

}
