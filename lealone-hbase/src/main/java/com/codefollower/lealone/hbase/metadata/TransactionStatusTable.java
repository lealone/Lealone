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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.hbase.transaction.Transaction;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;

public class TransactionStatusTable {
    private final static byte[] TABLE_NAME = Bytes.toBytes(MetaDataAdmin.META_DATA_PREFIX + "transaction_status_table");
    private final static byte[] ALL_LOCAL_TRANSACTION_NAMES = Bytes.toBytes("all_local_transaction_names");
    private final static byte[] COMMIT_TIMESTAMP = Bytes.toBytes("commit_timestamp");

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

    /**
     * 
     * @param localTransaction 参与者自己的本地事务
     * @param allLocalTransactionNames 所有参与者的本地事务名列表
     */
    public synchronized void addRecord(Transaction localTransaction, byte[] allLocalTransactionNames) {
        Put put = new Put(Bytes.toBytes(localTransaction.getTransactionName()));
        put.add(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, COMMIT_TIMESTAMP, localTransaction.getTransactionId(),
                Bytes.toBytes(localTransaction.getCommitTimestamp()));
        put.add(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, ALL_LOCAL_TRANSACTION_NAMES, localTransaction.getTransactionId(),
                allLocalTransactionNames);
        try {
            table.put(put);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    public long query(String hostAndPort, long queryTimestamp) {
        String rowKey = Transaction.getTransactionName(hostAndPort, queryTimestamp);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.setTimeStamp(queryTimestamp);
        try {
            long commitTimestamp = -1;
            Result r = table.get(get);
            if (r != null && !r.isEmpty()) {
                commitTimestamp = Bytes.toLong(r.getValue(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, COMMIT_TIMESTAMP));
                String serverStr = Bytes.toString(r.getValue(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, ALL_LOCAL_TRANSACTION_NAMES));
                String[] servers = serverStr.split(",");
                for (String server : servers) {
                    if (!rowKey.equals(server)) {
                        get = new Get(Bytes.toBytes(server));
                        if (!table.exists(get)) {
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
