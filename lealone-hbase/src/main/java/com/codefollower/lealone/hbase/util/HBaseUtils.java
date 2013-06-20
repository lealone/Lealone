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
package com.codefollower.lealone.hbase.util;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.command.dml.Select;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.hbase.command.dml.Task;
import com.codefollower.lealone.hbase.command.dml.WhereClauseSupport;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.engine.SessionRemotePool;
import com.codefollower.lealone.hbase.zookeeper.ZooKeeperAdmin;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.util.New;
import com.codefollower.lealone.util.StringUtils;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueBoolean;
import com.codefollower.lealone.value.ValueByte;
import com.codefollower.lealone.value.ValueBytes;
import com.codefollower.lealone.value.ValueDate;
import com.codefollower.lealone.value.ValueDecimal;
import com.codefollower.lealone.value.ValueDouble;
import com.codefollower.lealone.value.ValueFloat;
import com.codefollower.lealone.value.ValueInt;
import com.codefollower.lealone.value.ValueJavaObject;
import com.codefollower.lealone.value.ValueLong;
import com.codefollower.lealone.value.ValueNull;
import com.codefollower.lealone.value.ValueShort;
import com.codefollower.lealone.value.ValueString;
import com.codefollower.lealone.value.ValueStringFixed;
import com.codefollower.lealone.value.ValueStringIgnoreCase;
import com.codefollower.lealone.value.ValueTime;
import com.codefollower.lealone.value.ValueTimestamp;
import com.codefollower.lealone.value.ValueUuid;

public class HBaseUtils {
    public static final String HBASE_DB_NAME = "hbasedb";
    private static final Configuration conf = HBaseConfiguration.create();
    private static final Random random = new Random(System.currentTimeMillis());
    private static HConnection hConnection;
    private static HBaseAdmin admin;

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

    public static Value toValue(byte[] b, int type) {
        if (b == null)
            return ValueNull.INSTANCE;
        switch (type) {
        case Value.NULL:
            return ValueNull.INSTANCE;
        case Value.BYTES:
            return ValueBytes.get(b);
        case Value.UUID:
            return ValueUuid.get(toString(b));
        case Value.JAVA_OBJECT:
            return ValueJavaObject.get(b);
        case Value.BOOLEAN:
            return ValueBoolean.get(Bytes.toBoolean(b));
        case Value.BYTE:
            return ValueByte.get((byte) Bytes.toShort(b));
        case Value.DATE:
            return ValueDate.get(new Date(Bytes.toLong(b)));
        case Value.TIME:
            return ValueTime.get(new Time(Bytes.toLong(b)));
        case Value.TIMESTAMP:
            return ValueTimestamp.get(new Timestamp(Bytes.toLong(b)));
        case Value.DECIMAL:
            return ValueDecimal.get(Bytes.toBigDecimal(b));
        case Value.DOUBLE:
            return ValueDouble.get(Bytes.toDouble(b));
        case Value.FLOAT:
            return ValueFloat.get(Bytes.toFloat(b));
        case Value.INT:
            return ValueInt.get(Bytes.toInt(b));
        case Value.LONG:
            return ValueLong.get(Bytes.toLong(b));
        case Value.SHORT:
            return ValueShort.get(Bytes.toShort(b));
        case Value.STRING:
            return ValueString.get(toString(b));
        case Value.STRING_IGNORECASE:
            return ValueStringIgnoreCase.get(toString(b));
        case Value.STRING_FIXED:
            return ValueStringFixed.get(toString(b));
        case Value.BLOB:
            return ValueBytes.get(b);
        case Value.CLOB:
            return ValueBytes.get(b);
        case Value.ARRAY:
            return ValueBytes.get(b);
        case Value.RESULT_SET:
            return ValueBytes.get(b);
        default:
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "type=" + type);
        }
    }

    public static byte[] toBytes(Value v) {
        int type = v.getType();
        switch (type) {
        case Value.NULL:
            return toBytes("NULL");
        case Value.BYTES:
            return v.getBytes();
        case Value.UUID:
            return toBytes(v.getString());
        case Value.JAVA_OBJECT:
            return v.getBytes();
        case Value.BOOLEAN:
            return Bytes.toBytes(v.getBoolean());
        case Value.BYTE:
            return Bytes.toBytes(v.getShort());
        case Value.DATE:
            return Bytes.toBytes(v.getDate().getTime());
        case Value.TIME:
            return Bytes.toBytes(v.getTime().getTime());
        case Value.TIMESTAMP:
            return Bytes.toBytes(v.getTimestamp().getTime());
        case Value.DECIMAL:
            return Bytes.toBytes(v.getBigDecimal());
        case Value.DOUBLE:
            return Bytes.toBytes(v.getDouble());
        case Value.FLOAT:
            return Bytes.toBytes(v.getFloat());
        case Value.INT:
            return Bytes.toBytes(v.getInt());
        case Value.LONG:
            return Bytes.toBytes(v.getLong());
        case Value.SHORT:
            return Bytes.toBytes(v.getShort());
        case Value.STRING:
            return toBytes(v.getString());
        case Value.STRING_IGNORECASE:
            return toBytes(v.getString());
        case Value.STRING_FIXED:
            return toBytes(v.getString());
        case Value.BLOB:
            return v.getBytes();
        case Value.CLOB:
            return v.getBytes();
        case Value.ARRAY:
            return v.getBytes();
        case Value.RESULT_SET:
            return v.getBytes();
        default:
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "type=" + type);
        }
    }

    public static String createURL(HRegionLocation regionLocation) {
        return createURL(regionLocation.getHostname(), ZooKeeperAdmin.getTcpPort(regionLocation));
    }

    public static String createURL(ServerName sn) {
        return createURL(sn.getHostname(), ZooKeeperAdmin.getTcpPort(sn));
    }

    public static String createURL(String hostname, int port) {
        StringBuilder url = new StringBuilder(100);
        url.append("jdbc:lealone:tcp://").append(hostname).append(":").append(port).append("/").append(HBASE_DB_NAME);
        return url.toString();
    }

    public static HBaseAdmin getHBaseAdmin() throws IOException {
        if (admin == null) {
            synchronized (HBaseUtils.class) {
                if (admin == null) {
                    admin = new HBaseAdmin(getConfiguration());
                    if (hConnection == null || hConnection.isClosed()) {
                        hConnection = admin.getConnection();
                    }
                }
            }
        }
        return admin;
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

    public static void reset() throws IOException {
        if (hConnection != null) {
            hConnection.close();
            hConnection = null;
            admin = null;
        }
    }

    public static String getMasterURL() {
        return createURL(ZooKeeperAdmin.getMasterAddress());
    }

    public static ServerName getMasterServerName() {
        return ZooKeeperAdmin.getMasterAddress();
    }

    /**
     * 随机获取一个可用的RegionServer URL
     * 
     * @return
     * @throws IOException
     */
    public static String getRegionServerURL() throws IOException {
        List<ServerName> servers = ZooKeeperAdmin.getOnlineServers();
        ServerName sn = servers.get(random.nextInt(servers.size()));
        return createURL(sn);
    }

    public static String getRegionServerURL(String tableName, String rowKey) throws IOException {
        return getRegionServerURL(Bytes.toBytes(tableName), Bytes.toBytes(rowKey));
    }

    public static String getRegionServerURL(byte[] tableName, byte[] rowKey) throws IOException {
        HRegionLocation regionLocation = getConnection().locateRegion(tableName, rowKey);
        return createURL(regionLocation);
    }

    public static HBaseRegionInfo getHBaseRegionInfo(String tableName, String rowKey) {
        return getHBaseRegionInfo(Bytes.toBytes(tableName), Bytes.toBytes(rowKey));
    }

    public static HBaseRegionInfo getHBaseRegionInfo(byte[] tableName, byte[] rowKey) {
        try {
            HRegionLocation regionLocation = getConnection().locateRegion(tableName, rowKey);
            return new HBaseRegionInfo(regionLocation);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
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
                    Bytes.compareTo(startKeys[i], endKey) <= 0) { //原先代码是<=，因为coprocessorExec的语义是要包含endKey的
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

    public static NavigableMap<HRegionInfo, ServerName> getRegionLocations(byte[] tableName, byte[] startKey, byte[] endKey)
            throws IOException {
        NavigableMap<HRegionInfo, ServerName> regions = MetaScanner.allTableRegions(conf, tableName, false);

        if (startKey == null) {
            startKey = HConstants.EMPTY_START_ROW;
        }
        if (endKey == null) {
            endKey = HConstants.EMPTY_END_ROW;
        }

        NavigableMap<HRegionInfo, ServerName> result = new TreeMap<HRegionInfo, ServerName>();

        Set<HRegionInfo> keys = regions.keySet();
        byte[] start, end;
        for (HRegionInfo region : keys) {
            start = region.getStartKey();
            end = region.getEndKey();
            if (Bytes.compareTo(startKey, start) >= 0) {
                if (Bytes.equals(end, HConstants.EMPTY_END_ROW) || Bytes.compareTo(startKey, end) < 0) {
                    result.put(region, regions.get(region));
                }
            } else if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) || //
                    Bytes.compareTo(start, endKey) <= 0) { //原先代码是<=，因为coprocessorExec的语义是要包含endKey的
                result.put(region, regions.get(region));
            } else {
                break; // past stop
            }
        }
        return result;
    }

    public static boolean isLocal(HBaseSession session, HBaseRegionInfo hri) {
        if (hri == null)
            return false;
        ServerName sn = null;
        if (session.getMaster() != null)
            sn = HBaseUtils.getMasterServerName();
        else if (session.getRegionServer() != null)
            sn = session.getRegionServer().getServerName();
        if (sn == null)
            return false;

        if (hri.getHostname().equalsIgnoreCase(sn.getHostname()) && hri.getTcpPort() == ZooKeeperAdmin.getTcpPort(sn))
            return true;
        return false;
    }

    public static Task parseRowKey(HBaseSession session, WhereClauseSupport whereClauseSupport, Prepared prepared)
            throws Exception {

        byte[] tableName = whereClauseSupport.getTableNameAsBytes();
        Value startValue = whereClauseSupport.getStartRowKeyValue();
        Value endValue = whereClauseSupport.getEndRowKeyValue();

        String sql = prepared.getSQL();

        byte[] start = null;
        byte[] end = null;
        if (startValue != null)
            start = toBytes(startValue);
        if (endValue != null)
            end = toBytes(endValue);

        if (start == null)
            start = HConstants.EMPTY_START_ROW;
        if (end == null)
            end = HConstants.EMPTY_END_ROW;

        boolean oneRegion = false;
        List<byte[]> startKeys = null;
        if (startValue != null && endValue != null && startValue == endValue)
            oneRegion = true;

        if (!oneRegion) {
            startKeys = HBaseUtils.getStartKeysInRange(tableName, start, end);
            if (startKeys == null || startKeys.isEmpty()) {
                throw new RuntimeException("no regions for table: " + Bytes.toString(tableName) + " start: " + startValue
                        + " end: " + endValue);
            } else if (startKeys.size() == 1) {
                oneRegion = true;
                start = startKeys.get(0);
            }
        }

        Task task = new Task();

        if (oneRegion) {
            HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, start);
            if (isLocal(session, hri)) {
                task.localRegion = hri.getRegionName();
            } else {
                task.remoteCommand = SessionRemotePool.getCommandRemote(session, prepared.getParameters(),
                        hri.getRegionServerURL(), createSQL(hri.getRegionName(), sql));
            }
        } else {
            try {
                Map<String, List<HBaseRegionInfo>> servers = New.hashMap();
                List<HBaseRegionInfo> list;
                for (byte[] startKey : startKeys) {
                    HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, startKey);
                    if (HBaseUtils.isLocal(session, hri)) {
                        if (task.localRegions == null)
                            task.localRegions = New.arrayList();

                        task.localRegions.add(hri.getRegionName());
                    } else {
                        list = servers.get(hri.getRegionServerURL());
                        if (list == null) {
                            list = New.arrayList();
                            servers.put(hri.getRegionServerURL(), list);
                        }
                        list.add(hri);
                    }
                }

                String planSQL = sql;
                if (prepared.isQuery()) {
                    Select select = (Select) prepared;
                    if (select.isGroupQuery())
                        planSQL = select.getPlanSQL(true);
                    else if (select.getSortOrder() != null && select.getOffset() != null) //分布式排序时不使用Offset
                        planSQL = select.getPlanSQL(false, false);
                }

                for (Map.Entry<String, List<HBaseRegionInfo>> e : servers.entrySet()) {
                    if (task.remoteCommands == null)
                        task.remoteCommands = New.arrayList();
                    task.remoteCommands.add(SessionRemotePool.getCommandRemote(session, prepared.getParameters(), e.getKey(),
                            HBaseUtils.createSQL(e.getValue(), planSQL)));
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return task;
    }

    public static String createSQL(String regionName, String sql) {
        StringBuilder buff = new StringBuilder("IN THE REGION ");
        buff.append(StringUtils.quoteStringSQL(regionName)).append(" ").append(sql);
        return buff.toString();
    }

    public static String createSQL(List<HBaseRegionInfo> list, String sql) {
        StringBuilder buff = new StringBuilder("IN THE REGION ");
        for (int i = 0, size = list.size(); i < size; i++) {
            if (i > 0)
                buff.append(',');
            buff.append(StringUtils.quoteStringSQL(list.get(i).getRegionName()));
        }
        buff.append(" ").append(sql);
        return buff.toString();
    }
}
