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
import java.util.ArrayList;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.transaction.Transaction.CommitInfo;

public class TransactionStatusTable {
    private final static byte[] TABLE_NAME = Bytes.toBytes(MetaDataAdmin.META_DATA_PREFIX + "transaction_status_table");
    private final static byte[] SERVERS = Bytes.toBytes("s");
    private final static byte[] COMMIT_TIMESTAMP = Bytes.toBytes("c");
    //private final static byte[] IS_NESTED = Bytes.toBytes("n");

    private final static TransactionStatusTable st = new TransactionStatusTable();

    public static TransactionStatusTable getInstance() {
        return st;
    }

    private final HTable table;

    private TransactionStatusTable() {
        try {
            MetaDataAdmin.createTableIfNotExists(TABLE_NAME);
            table = new HTable(HBaseUtils.getConfiguration(), TABLE_NAME);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public void addRecord(HBaseSession session) {
        CommitInfo[] allCommitInfo = session.getRootTransaction().getAllCommitInfo(session.isRegionServer());
        int len = allCommitInfo.length;
        if (len > 0) {
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < len; i++) {
                for (String key : allCommitInfo[i].getKeys()) {
                    buff.append(key);
                    buff.append(',');
                }
            }
            buff.setLength(buff.length() - 1);
            String serverStr = buff.toString();

            ArrayList<Put> puts = new ArrayList<Put>();
            byte[] rowKey;
            Put put;
            long tid;

            CommitInfo commitInfo;
            long[] transactionIds;
            long[] commitTimestamps;

            for (int i = 0; i < len; i++) {
                commitInfo = allCommitInfo[i];
                transactionIds = commitInfo.getTransactionIds();
                commitTimestamps = commitInfo.getCommitTimestamps();
                for (int j = 0, size = transactionIds.length; j < size; j++) {
                    tid = transactionIds[j];
                    rowKey = Bytes.toBytes(commitInfo.getKey(tid));

                    put = new Put(rowKey);
                    //put.add(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, IS_NESTED, tid, Bytes.toBytes(t.isNested()));
                    put.add(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, COMMIT_TIMESTAMP, tid, Bytes.toBytes(commitTimestamps[j]));
                    put.add(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, SERVERS, tid, Bytes.toBytes(serverStr));
                    puts.add(put);
                }
            }

            try {
                table.put(puts);
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }
    }

    public long query(String hostAndPort, long queryTimestamp) {
        String rowKey = CommitInfo.getKey(hostAndPort, queryTimestamp);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.setTimeStamp(queryTimestamp);
        try {
            long commitTimestamp = -1;
            Result r = table.get(get);
            if (r != null && !r.isEmpty()) {
                commitTimestamp = Bytes.toLong(r.getValue(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, COMMIT_TIMESTAMP));
                String serverStr = Bytes.toString(r.getValue(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, SERVERS));
                String[] servers = serverStr.split(",");
                for (String server : servers) {
                    if (!rowKey.equals(server)) {
                        get = new Get(Bytes.toBytes(server));
                        r = table.get(get);
                        if (r == null || r.isEmpty()) {
                            commitTimestamp = -1;
                            break;
                        }
                    }
                }
            }
            return commitTimestamp;
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }
}
