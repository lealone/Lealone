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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MurmurHash;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;

public class HBaseTransactionStatusTable {
    private final static byte[] TABLE_NAME = Bytes.toBytes("LEALONE_TRANSACTION_STATUS_TABLE");
    final static byte[] FAMILY = Bytes.toBytes("f");
    final static byte[] START_TIMESTAMP = Bytes.toBytes("s");
    final static byte[] COMMIT_TIMESTAMP = Bytes.toBytes("c");

    public synchronized static void createTableIfNotExists() throws Exception {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
        hcd.setMaxVersions(Integer.MAX_VALUE);

        HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
        htd.addFamily(hcd);
        if (!admin.tableExists(TABLE_NAME)) {
            admin.createTable(htd);
        }
    }

    public synchronized static void dropTableIfExists() throws Exception {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        if (admin.tableExists(TABLE_NAME)) {
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        }
    }

    private final HTable table;

    public HBaseTransactionStatusTable() {
        try {
            createTableIfNotExists();
            table = new HTable(HBaseUtils.getConfiguration(), TABLE_NAME);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public void addRecord(long transactionId, long commitTimestamp) {
        byte[] tid = Bytes.toBytes(transactionId);
        int rowKey = MurmurHash.getInstance().hash(tid, 0, tid.length, 0xdeadbeef);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(FAMILY, START_TIMESTAMP, tid);
        put.add(FAMILY, COMMIT_TIMESTAMP, Bytes.toBytes(commitTimestamp));
        try {
            table.put(put);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    public byte[] toHash(long transactionId) {
        byte[] tid = Bytes.toBytes(transactionId);
        int rowKey = MurmurHash.getInstance().hash(tid, 0, tid.length, 0xdeadbeef);
        return Bytes.toBytes(rowKey);
    }
}
