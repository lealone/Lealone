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
package com.codefollower.lealone.hbase.metadata;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.hbase.util.HBaseUtils;

public class TimestampServiceTable {
    private final static byte[] TABLE_NAME = Bytes.toBytes(MetaDataAdmin.META_DATA_PREFIX + "timestamp_service_table");
    private final static byte[] FAMILY = Bytes.toBytes("f");
    //private final static byte[] HOST_AND_PORT = Bytes.toBytes("host_and_port");
    private final static byte[] LAST_MAX_TIMESTAMP = Bytes.toBytes("last_max_timestamp");

    public synchronized static void createTableIfNotExists() throws IOException {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        if (!admin.tableExists(TABLE_NAME)) {
            HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
            hcd.setMaxVersions(1); //只需要保留最新版本即可

            HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
            htd.addFamily(hcd);
            admin.createTable(htd);
        }
    }

    public synchronized static void dropTableIfExists() throws IOException {
        MetaDataAdmin.dropTableIfExists(TABLE_NAME);
    }

    private final byte[] hostAndPort;
    private final HTable table;

    public TimestampServiceTable(String hostAndPort) throws IOException {
        createTableIfNotExists();
        this.hostAndPort = Bytes.toBytes(hostAndPort);
        table = new HTable(HBaseUtils.getConfiguration(), TABLE_NAME);
    }

    public void updateLastMaxTimestamp(long lastMaxTimestamp) throws IOException {
        Put put = new Put(hostAndPort);
        put.add(FAMILY, LAST_MAX_TIMESTAMP, Bytes.toBytes(lastMaxTimestamp));
        table.put(put);
    }

    public long getLastMaxTimestamp() throws IOException {
        long lastMaxTimestamp = 0;
        Get get = new Get(hostAndPort);
        Result r = table.get(get);
        if (r != null && !r.isEmpty()) {
            lastMaxTimestamp = Bytes.toLong(r.getValue(FAMILY, LAST_MAX_TIMESTAMP));
        }

        return lastMaxTimestamp;
    }
}
