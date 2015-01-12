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
package org.lealone.hbase.transaction;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.lealone.engine.Session;
import org.lealone.hbase.engine.HBaseConstants;
import org.lealone.hbase.result.HBaseRow;
import org.lealone.hbase.util.HBaseUtils;
import org.lealone.transaction.GlobalTransaction;
import org.lealone.transaction.TransactionManager;
import org.lealone.value.Value;

public class HBaseTransaction extends GlobalTransaction {
    private final byte[] transactionMetaAdd;
    private final byte[] transactionMetaDelete;

    public HBaseTransaction(Session session) {
        super(session);
        String hostAndPort = TransactionManager.getHostAndPort();

        transactionMetaAdd = Bytes.toBytes(hostAndPort + "," + transactionId + "," + HBaseConstants.Tag.ADD);
        transactionMetaDelete = Bytes.toBytes(hostAndPort + "," + transactionId + "," + HBaseConstants.Tag.DELETE);
    }

    @Override
    public String toString() {
        return "T-" + transactionId;
    }

    public void log(HBaseRow row) {
        if (!autoCommit)
            undoRows.add(row);
    }

    public Put createHBasePut(byte[] defaultColumnFamilyName, Value rowKey) {
        Put put = new Put(HBaseUtils.toBytes(rowKey), getNewTimestamp());
        put.add(defaultColumnFamilyName, HBaseConstants.TRANSACTION_META, transactionMetaAdd);
        return put;
    }

    public Put createHBasePutWithDeleteTag(byte[] defaultColumnFamilyName, byte[] rowKey) {
        Put put = new Put(rowKey, getNewTimestamp());
        put.add(defaultColumnFamilyName, HBaseConstants.TRANSACTION_META, transactionMetaDelete);
        return put;
    }

    @Override
    public void addHalfSuccessfulTransaction(Long tid) {
        halfSuccessfulTransactions.add(tid);
    }

    public static String getTransactionName(String hostAndPort, long tid) {
        StringBuilder buff = new StringBuilder(hostAndPort);
        buff.append(':');
        buff.append(tid);
        return buff.toString();
    }

}
