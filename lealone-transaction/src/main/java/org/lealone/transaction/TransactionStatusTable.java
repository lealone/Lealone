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

import org.lealone.command.router.FrontendSessionPool;
import org.lealone.engine.Constants;
import org.lealone.engine.FrontendSession;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.mvstore.MVMap;
import org.lealone.mvstore.MVStore;
import org.lealone.util.New;

public class TransactionStatusTable {
    private TransactionStatusTable() {
    }

    //HBase默认情况下只有当前region server的hostAndPort，
    //但是当发生split时原有记录的hostAndPort没变，只不过记录被移到了当前region server，
    //为了使得事务状态表中的记录仍然有效，所以还是用原有记录的hostAndPort
    private final static Map<String, TransactionStatusCache> hostAndPortMap = New.hashMap();
    /**
     * The persisted map of transactionStatusTable.
     * Key: transaction_name, value: [ all_local_transaction_names, commit_timestamp ].
     */
    private static MVMap<String, Object[]> map;

    synchronized static void init(MVStore store) {
        if (map != null)
            return;
        map = store.openMap("transactionStatusTable", new MVMap.Builder<String, Object[]>());
    }

    public static void commit(TransactionBase transaction, String allLocalTransactionNames) {
        Object[] v = { allLocalTransactionNames, transaction.getCommitTimestamp() };
        map.put(transaction.getTransactionName(), v);
    }

    //TODO
    synchronized static boolean isFullSuccessful(String hostAndPort, long tid) {
        return true;
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
    public static boolean isValid(Session session, String hostAndPort, long oldTid, TransactionInterface currentTransaction) {
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

        String oldTransactionName = GlobalTransaction.getTransactionName(hostAndPort, oldTid);

        Object[] v = map.get(oldTransactionName);

        commitTimestamp = (long) v[1];
        String[] allLocalTransactionNames = ((String) v[0]).split(",");
        boolean isFullSuccessful = true;

        for (String localTransactionName : allLocalTransactionNames) {
            if (!oldTransactionName.equals(localTransactionName)) {
                if (!validate(session, localTransactionName)) {
                    isFullSuccessful = false;
                    break;
                }
            }
        }

        //TODO 如果前一个事务没有结束，如何让它结束或是等它结束。
        if (isFullSuccessful) {
            cache.set(oldTid, commitTimestamp);
            return true;
        } else {
            cache.set(oldTid, -2);
            return false;
        }
    }

    public static boolean validate(Session session, String localTransactionName) {
        String[] a = localTransactionName.split(":");

        FrontendSession fs = null;
        try {
            fs = FrontendSessionPool.getFrontendSession(session.getOriginalProperties(), createURL(session, a[0], a[1]));
            return fs.validateTransaction(localTransactionName);
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            FrontendSessionPool.release(fs);
        }
    }

    private static String createURL(Session session, String host, String port) {
        StringBuilder url = new StringBuilder(100);
        url.append(Constants.URL_PREFIX).append(Constants.URL_TCP).append("//");
        url.append(host).append(":").append(port);
        url.append("/").append(session.getDatabase().getShortName());
        return url.toString();
    }

    public static boolean isValid(String localTransactionName) {
        return map.containsKey(localTransactionName);
    }
}
