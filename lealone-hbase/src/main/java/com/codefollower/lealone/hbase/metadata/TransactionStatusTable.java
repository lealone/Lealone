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
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.transaction.Transaction;

public class TransactionStatusTable {
    private final static byte[] TABLE_NAME = Bytes.toBytes(MetaDataAdmin.META_DATA_PREFIX + "transaction_status_table");
    //final static byte[] START_TIMESTAMP = Bytes.toBytes("t");
    private final static byte[] SERVER = Bytes.toBytes("s");
    private final static byte[] COMMIT_TIMESTAMP = Bytes.toBytes("c");

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

    public void addRecord(Transaction transaction, boolean isMaster) {
        Set<Transaction> transactions = transaction.getChildren();
        if (!isMaster)
            transactions.add(transaction);
        if (transactions != null && !transactions.isEmpty()) {
            StringBuilder buff = new StringBuilder();
            for (Transaction t : transactions) {
                if (buff.length() > 0)
                    buff.append(',');

                buff.append(t.getHostAndPort()).append(':').append(t.getTransactionId());
            }

            ArrayList<Put> list = new ArrayList<Put>(transactions.size());
            String serverStr = buff.toString();

            byte[] rowKey;
            Put put;

            for (Transaction t : transactions) {
                buff.setLength(0);
                rowKey = Bytes.toBytes(buff.append(t.getHostAndPort()).append(':').append(t.getTransactionId()).toString());
                put = new Put(rowKey);
                put.add(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, SERVER, t.getTransactionId(), Bytes.toBytes(serverStr));
                put.add(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, COMMIT_TIMESTAMP, t.getTransactionId(),
                        Bytes.toBytes(t.getCommitTimestamp()));
                list.add(put);
            }

            try {
                table.put(list);
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }
    }

    public long query(String hostAndPort, long queryTimestamp) {
        Get get = new Get(Bytes.toBytes(hostAndPort + "." + Long.toString(queryTimestamp)));
        get.setTimeStamp(queryTimestamp);
        try {
            long commitTimestamp = -1;
            Result r = table.get(get);
            if (r != null && !r.isEmpty()) {
                commitTimestamp = Bytes.toLong(r.getValue(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, COMMIT_TIMESTAMP));
                String serverStr = Bytes.toString(r.getValue(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, SERVER));
                String[] servers = serverStr.split(",");
                for (String server : servers) {
                    get = new Get(Bytes.toBytes(server));
                    r = table.get(get);
                    if (r == null || r.isEmpty()) {
                        commitTimestamp = -1;
                        break;
                    }
                }
            }
            return commitTimestamp;
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }
}
