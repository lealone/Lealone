/*
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
package org.lealone.hbase.metadata;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lealone.hbase.transaction.HBaseTransaction;
import org.lealone.hbase.util.HBaseUtils;
import org.lealone.message.DbException;
import org.lealone.transaction.TransactionStatusCache;
import org.lealone.util.New;

public class TransactionStatusTable {
    private final static byte[] TABLE_NAME = Bytes.toBytes(MetaDataAdmin.META_DATA_PREFIX + "transaction_status_table");
    private final static byte[] ALL_LOCAL_TRANSACTION_NAMES = Bytes.toBytes("all_local_transaction_names");
    private final static byte[] COMMIT_TIMESTAMP = Bytes.toBytes("commit_timestamp");

    //默认情况下只有当前region server的hostAndPort，
    //但是当发生split时原有记录的hostAndPort没变，只不过记录被移到了当前region server，
    //为了使得事务状态表中的记录仍然有效，所以还是用原有记录的hostAndPort
    private final static Map<String, TransactionStatusCache> hostAndPortMap = New.hashMap();

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
    public synchronized void addRecord(HBaseTransaction localTransaction, byte[] allLocalTransactionNames) {
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

    private static TransactionStatusCache newCache(String hostAndPort) {
        synchronized (TransactionStatusTable.class) {
            TransactionStatusCache cache = hostAndPortMap.get(hostAndPort);
            if (cache == null) {
                cache = new TransactionStatusCache();
                hostAndPortMap.put(hostAndPort, cache);
            }

            return cache;
        }
    }

    /**
     * 检查事务是否有效
     * 
     * @param hostAndPort 所要检查的行所在的主机名和端口号
     * @param oldTid 所要检查的行存入数据库的旧事务id
     * @param currentTransaction 当前事务
     * @return true 有效 
     */
    public boolean isValid(String hostAndPort, long oldTid, HBaseTransaction currentTransaction) {
        TransactionStatusCache cache = hostAndPortMap.get(hostAndPort);
        if (cache == null) {
            cache = newCache(hostAndPort);
        }
        long commitTimestamp = cache.get(oldTid);
        //1.上一次已经查过了，已确认过是条无效的记录
        if (commitTimestamp == -2)
            return false;
        //2. 是有效的事务记录，再进一步判断是否小于等于当前事务的开始时间戳
        if (commitTimestamp != -1)
            return commitTimestamp <= currentTransaction.getTransactionId();

        String oldTransactionName = HBaseTransaction.getTransactionName(hostAndPort, oldTid);
        Get get = new Get(Bytes.toBytes(oldTransactionName));
        try {
            Result r = table.get(get);
            if (r != null && !r.isEmpty()) {
                boolean isFullSuccessful = true;
                commitTimestamp = Bytes.toLong(r.getValue(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, COMMIT_TIMESTAMP));
                String[] allLocalTransactionNames = Bytes.toString(
                        r.getValue(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, ALL_LOCAL_TRANSACTION_NAMES)).split(",");
                for (String localTransactionName : allLocalTransactionNames) {
                    if (!oldTransactionName.equals(localTransactionName)) {
                        get = new Get(Bytes.toBytes(localTransactionName));
                        if (!table.exists(get)) {
                            isFullSuccessful = false;
                            break;
                        }
                    }
                }

                if (isFullSuccessful)
                    cache.set(oldTid, commitTimestamp);

                if (commitTimestamp <= currentTransaction.getTransactionId()) {
                    if (!isFullSuccessful)
                        currentTransaction.addHalfSuccessfulTransaction(oldTid);

                    return true;
                } else {
                    return false;
                }
            } else {
                cache.set(oldTid, -2);
                return false;
            }
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    public boolean isFullSuccessful(String hostAndPort, long tid) {
        TransactionStatusCache cache = hostAndPortMap.get(hostAndPort);
        if (cache == null) {
            cache = newCache(hostAndPort);
        }
        if (cache.get(tid) > 0)
            return true;

        String transactionName = HBaseTransaction.getTransactionName(hostAndPort, tid);
        Get get = new Get(Bytes.toBytes(transactionName));
        try {
            Result r = table.get(get);
            if (r != null && !r.isEmpty()) {
                String[] allLocalTransactionNames = Bytes.toString(
                        r.getValue(MetaDataAdmin.DEFAULT_COLUMN_FAMILY, ALL_LOCAL_TRANSACTION_NAMES)).split(",");
                for (String localTransactionName : allLocalTransactionNames) {
                    if (!transactionName.equals(localTransactionName)) {
                        get = new Get(Bytes.toBytes(localTransactionName));
                        if (!table.exists(get)) {
                            return false;
                        }
                    }
                }

                return true;
            }
        } catch (IOException e) {
            return false;
        }

        return false;
    }
}
