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
package com.codefollower.lealone.omid.tso.persistence;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.codefollower.lealone.omid.tso.persistence.LoggerAsyncCallback.LoggerInitCallback;
import com.codefollower.lealone.omid.tso.persistence.LoggerException.Code;

public class HBaseStateLogger implements StateLogger {
    final static byte[] FAMILY = Bytes.toBytes("f");
    final static byte[] COLUMN = Bytes.toBytes("c");
    private final static byte[] ZERO = { (byte) 0 };

    private HTable table;

    public HBaseStateLogger() {
        try {
            createTableIfNotExists("LEALONE_STATE_LOGGER");

            table = new HTable(HBaseConfiguration.create(), "LEALONE_STATE_LOGGER");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initialize(LoggerInitCallback cb, Object ctx) throws LoggerException {
        // TODO Auto-generated method stub

    }

    @Override
    public void addRecord(byte[] record, AddRecordCallback cb, Object ctx) {
        Put put = new Put(record);
        put.add(FAMILY, FAMILY, ZERO);
        try {
            table.put(put);
            cb.addRecordComplete(Code.OK, ctx);
        } catch (IOException e) {
            cb.addRecordComplete(Code.ADDFAILED, ctx);
        }
    }

    @Override
    public void shutdown() {
        try {
            table.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public synchronized static void createTableIfNotExists(String tableName) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
        HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
        hcd.setMaxVersions(1);

        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(hcd);
        if (!admin.tableExists(tableName)) {
            admin.createTable(htd);
        }
    }

    public synchronized static void dropTableIfExists(String tableName) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }
}
