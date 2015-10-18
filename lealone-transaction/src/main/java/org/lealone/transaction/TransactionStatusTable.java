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
package org.lealone.transaction;

import java.util.Map;

import org.lealone.common.util.New;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StringDataType;
import org.lealone.transaction.log.LogMap;
import org.lealone.transaction.log.LogStorage;

class TransactionStatusTable {
    private TransactionStatusTable() {
    }

    private final static Map<String, TransactionStatusCache> hostAndPortMap = New.hashMap();

    /**
     * The persisted map of transactionStatusTable.
     * Key: transactionName, value: [ allLocalTransactionNames, commitTimestamp ].
     */
    private static LogMap<String, Object[]> map;

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

    static synchronized void init(LogStorage logStorage) {
        if (map == null) {
            map = logStorage.openLogMap("transactionStatusTable", StringDataType.INSTANCE, new ObjectDataType());
        }
    }

    static void commit(MVCCTransaction transaction, String allLocalTransactionNames) {
        Object[] v = { allLocalTransactionNames, transaction.getCommitTimestamp() };
        map.put(transaction.transactionName, v);
    }

    static boolean validateTransaction(String localTransactionName) {
        return map.containsKey(localTransactionName);
    }

    /**
     * 检查事务是否有效
     * 
     * @param hostAndPort 要检查的行所在的主机名和端口号
     * @param oldTid 要检查的行存入数据库的旧事务id
     * @param currentTransaction 当前事务
     * @return true 有效 
     */
    static boolean validateTransaction(String hostAndPort, long oldTid, MVCCTransaction currentTransaction) {
        TransactionStatusCache cache = hostAndPortMap.get(hostAndPort);
        if (cache == null) {
            cache = newCache(hostAndPort);
        }
        long commitTimestamp = cache.get(oldTid);
        // 1.上一次已经查过了，已确认过是条无效的记录
        if (commitTimestamp == -2)
            return false;
        // 2. 是有效的事务记录，再进一步判断是否小于等于当前事务的开始时间戳
        if (commitTimestamp != -1)
            return commitTimestamp <= currentTransaction.transactionId;

        String oldTransactionName = MVCCTransaction.getTransactionName(hostAndPort, oldTid);

        Object[] v = map.get(oldTransactionName);
        if (v == null) // TODO
            return true;

        commitTimestamp = (long) v[1];
        String[] allLocalTransactionNames = ((String) v[0]).split(",");
        boolean isFullSuccessful = true;

        for (String localTransactionName : allLocalTransactionNames) {
            if (!oldTransactionName.equals(localTransactionName)) {
                if (!currentTransaction.validator.validateTransaction(localTransactionName)) {
                    isFullSuccessful = false;
                    break;
                }
            }
        }

        // TODO 如果前一个事务没有结束，如何让它结束或是等它结束。
        if (isFullSuccessful) {
            cache.set(oldTid, commitTimestamp);
            return true;
        } else {
            cache.set(oldTid, -2);
            return false;
        }
    }
}
