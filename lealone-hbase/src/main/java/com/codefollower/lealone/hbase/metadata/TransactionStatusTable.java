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
     * @param participantLocalTransactions 参与者自己的本地事务
     * @param allLocalTransactionNames 所有参与者的本地事务名列表
     * @param commitTimestamp 参与者本地事务的提交时间，参与者如果有多个本地事务，这些本地事务用同样的提交时间
     */
    public synchronized void addRecord(ArrayList<Transaction> participantLocalTransactions, byte[] allLocalTransactionNames,
            byte[] commitTimestamp) {
        int size = participantLocalTransactions.size();
        ArrayList<Put> puts = new ArrayList<Put>(size);
        Transaction t;
        Put put;

        for (int i = 0; i < size; i++) {
            t = participantLocalTransactions.get(i);
            put = new Put(Bytes.toBytes(t.getTransactionName()));
            put.add(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, COMMIT_TIMESTAMP, t.getTransactionId(), commitTimestamp);
            put.add(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, ALL_LOCAL_TRANSACTION_NAMES, t.getTransactionId(),
                    allLocalTransactionNames);
            puts.add(put);
        }

        try {
            table.put(puts);
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
