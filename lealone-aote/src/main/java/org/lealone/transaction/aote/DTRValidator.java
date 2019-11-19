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
package org.lealone.transaction.aote;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.Constants;
import org.lealone.net.NetNode;
import org.lealone.transaction.Transaction.Validator;

//效验分布式事务和复制是否成功
//DT表示Distributed Transaction，R表示Replication
class DTRValidator extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(DTRValidator.class);

    private static final Map<String, DTStatusCache> hostAndPortMap = new HashMap<>();

    // key: transactionName, value: [ allLocalTransactionNames, commitTimestamp ].
    private static final ConcurrentHashMap<String, Object[]> dTransactions = new ConcurrentHashMap<>();

    // key: replicationName, value: replicationName.
    private static final ConcurrentHashMap<String, String> replications = new ConcurrentHashMap<>();

    private static final QueuedMessage closeSentinel = new QueuedMessage(null, null);
    private static final BlockingQueue<QueuedMessage> backlog = new LinkedBlockingQueue<>();

    private static final DTRValidator instance = new DTRValidator();

    static DTRValidator getInstance() {
        return instance;
    }

    private static class QueuedMessage {
        final AOTransaction t;
        final String allLocalTransactionNames;

        QueuedMessage(AOTransaction t, String allLocalTransactionNames) {
            this.t = t;
            this.allLocalTransactionNames = allLocalTransactionNames;
        }
    }

    private static DTStatusCache newCache(String hostAndPort) {
        synchronized (DTRValidator.class) {
            DTStatusCache cache = hostAndPortMap.get(hostAndPort);
            if (cache == null) {
                cache = new DTStatusCache();
                hostAndPortMap.put(hostAndPort, cache);
            }
            return cache;
        }
    }

    static void addTransaction(AOTransaction transaction, String allLocalTransactionNames) {
        Object[] v = { allLocalTransactionNames, transaction.getCommitTimestamp() };
        dTransactions.put(transaction.transactionName, v);
        try {
            backlog.put(new QueuedMessage(transaction, allLocalTransactionNames));
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private static void validateTransaction(QueuedMessage qm) {
        String[] allLocalTransactionNames = qm.allLocalTransactionNames.split(",");
        boolean isFullSuccessful = true;
        String localHostAndPort = NetNode.getLocalTcpHostAndPort();
        for (String localTransactionName : allLocalTransactionNames) {
            if (!localTransactionName.startsWith(localHostAndPort)) {
                if (!qm.t.validator.validate(localTransactionName)) {
                    isFullSuccessful = false;
                    break;
                }
            }
        }
        if (isFullSuccessful) {
            qm.t.commitAfterValidate(qm.t.transactionId);
        }
    }

    static boolean validateTransaction(String localTransactionName) {
        if (localTransactionName.startsWith("replication:"))
            return replications.containsKey(localTransactionName.substring("replication:".length()));
        else
            return dTransactions.containsKey(localTransactionName);
    }

    /**
     * 检查事务是否有效
     * 
     * @param hostAndPort 要检查的行所在的主机名和端口号
     * @param oldTid 要检查的行存入数据库的旧事务id
     * @param currentTransaction 当前事务
     * @return true 有效 
     */
    static boolean validateTransaction(String hostAndPort, long oldTid, AOTransaction currentTransaction) {
        DTStatusCache cache = hostAndPortMap.get(hostAndPort);
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

        String oldTransactionName = AOTransaction.getTransactionName(hostAndPort, oldTid);

        Object[] v = dTransactions.get(oldTransactionName);
        if (v == null) // TODO
            return true;

        commitTimestamp = (long) v[1];
        String[] allLocalTransactionNames = ((String) v[0]).split(",");
        boolean isFullSuccessful = true;

        for (String localTransactionName : allLocalTransactionNames) {
            if (!oldTransactionName.equals(localTransactionName)) {
                if (!currentTransaction.validator.validate(localTransactionName)) {
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

    static void addReplication(String replicationName) {
        replications.put(replicationName, replicationName);
    }

    static boolean containsReplication(String replicationName) {
        return replications.containsKey(replicationName);
    }

    static void removeReplication(String replicationName) {
        replications.remove(replicationName);
    }

    static boolean validateReplication(String replicationName, Validator validator) {
        boolean isValid = true;
        String[] names = replicationName.split(",");
        NetNode localHostAndPort = NetNode.getLocalTcpNode();
        // 从1开始，第一个不是节点名
        for (int i = 1, size = names.length; i < size && isValid; i++) {
            String name = names[i];
            if (name.indexOf(':') == -1) {
                name += ":" + Constants.DEFAULT_TCP_PORT;
            }

            if (localHostAndPort.equals(NetNode.createTCP(name)))
                continue;
            isValid = isValid && validator.validate(name, "replication:" + replicationName);
        }
        return isValid;
    }

    private DTRValidator() {
        super(DTRValidator.class.getSimpleName());
        setDaemon(true);
    }

    void close() {
        backlog.clear();
        backlog.add(closeSentinel);
    }

    @Override
    public void run() {
        int maxElements = 32;
        List<QueuedMessage> drainedMessages = new ArrayList<>(maxElements);
        while (true) {
            if (backlog.drainTo(drainedMessages, maxElements) == 0) {
                try {
                    drainedMessages.add(backlog.take());
                } catch (InterruptedException e) {
                    logger.warn("Failed to take queued message", e);
                }
            }
            for (QueuedMessage qm : drainedMessages) {
                if (closeSentinel == qm)
                    return;
                try {
                    validateTransaction(qm);
                } catch (Throwable e) {
                    logger.warn("Failed to validate transaction: " + qm.t, e);
                }
            }
            drainedMessages.clear();
        }
    }
}
