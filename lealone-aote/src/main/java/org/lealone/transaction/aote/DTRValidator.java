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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.Constants;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.session.Session;
import org.lealone.net.NetNode;
import org.lealone.server.protocol.dt.DistributedTransactionValidate;
import org.lealone.server.protocol.dt.DistributedTransactionValidateAck;
import org.lealone.server.protocol.replication.ReplicationCheckConflict;
import org.lealone.server.protocol.replication.ReplicationCheckConflictAck;
import org.lealone.storage.replication.ConsistencyLevel;

//效验分布式事务和复制是否成功
//DT表示Distributed Transaction，R表示Replication
class DTRValidator {

    private static final Map<String, DTStatusCache> hostAndPortMap = new HashMap<>();

    // key: transactionName, value: [ allLocalTransactionNames, commitTimestamp ].
    private static final ConcurrentHashMap<String, Object[]> dTransactions = new ConcurrentHashMap<>();

    // key: replicationName, value: replicationName.
    private static final ConcurrentHashMap<String, String> replications = new ConcurrentHashMap<>();

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
        validateTransactionAsync(transaction, allLocalTransactionNames.split(","));
    }

    private static void validateTransactionAsync(AOTransaction transaction, String[] allLocalTransactionNames) {
        AtomicBoolean isFullSuccessful = new AtomicBoolean(true);
        AtomicInteger size = new AtomicInteger(allLocalTransactionNames.length);
        AsyncHandler<DistributedTransactionValidateAck> handler = ack -> {
            isFullSuccessful.compareAndSet(true, ack.isValid);
            int index = size.decrementAndGet();
            if (index == 0 && isFullSuccessful.get()) {
                transaction.commitAfterValidate(transaction.transactionId);
            }
        };
        String localHostAndPort = NetNode.getLocalTcpHostAndPort();
        for (String localTransactionName : allLocalTransactionNames) {
            if (!localTransactionName.startsWith(localHostAndPort)) {
                String[] a = localTransactionName.split(":");
                String hostAndPort = a[0] + ":" + a[1];
                DistributedTransactionValidate packet = new DistributedTransactionValidate(localTransactionName);
                transaction.getSession().sendAsync(packet, hostAndPort, handler);
            } else {
                size.decrementAndGet();
            }
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
                if (!validateRemoteTransaction(localTransactionName, currentTransaction.getSession())) {
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

    private static boolean validateRemoteTransaction(String localTransactionName, Session session) {
        String[] a = localTransactionName.split(":");
        String hostAndPort = a[0] + ":" + a[1];
        return validateRemoteTransaction(hostAndPort, localTransactionName, session);
    }

    private static boolean validateRemoteTransaction(String hostAndPort, String localTransactionName, Session session) {
        DistributedTransactionValidate packet = new DistributedTransactionValidate(localTransactionName);
        DistributedTransactionValidateAck ack = session.sendSync(packet, hostAndPort);
        return ack.isValid;
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

    static boolean validateReplication(String replicationName, Session session) {
        int validNodes = 0;
        String[] names = replicationName.split(",");
        NetNode localHostAndPort = NetNode.getLocalTcpNode();
        ConsistencyLevel consistencyLevel = ConsistencyLevel.getLevel(names[1]);
        int size;
        if (consistencyLevel == ConsistencyLevel.ALL)
            size = names.length - 2;
        else
            size = (names.length - 2) / 2 + 1;

        // 从2开始，前两个不是节点名
        for (int i = 2, length = names.length; i < length; i++) {
            String name = names[i];
            if (name.indexOf(':') == -1) {
                name += ":" + Constants.DEFAULT_TCP_PORT;
            }
            if (localHostAndPort.equals(NetNode.createTCP(name))) {
                if (DTRValidator.containsReplication(replicationName)) {
                    if (++validNodes >= size)
                        return true;
                }
            } else if (validateRemoteTransaction(name, "replication:" + replicationName, session)) {
                if (++validNodes >= size)
                    return true;
            }
        }
        return false;
    }

    static String handleReplicationConflict(String mapName, ByteBuffer key, String replicationName, Session session) {
        // 第一步: 获取各节点的锁占用情况
        String[] names = replicationName.split(",");
        boolean[] local = new boolean[names.length];
        NetNode localHostAndPort = NetNode.getLocalTcpNode();
        int size = names.length - 2;
        String[] replicationNames = new String[size];
        AtomicInteger replicationNameIndex = new AtomicInteger();

        AsyncHandler<ReplicationCheckConflictAck> handler = ack -> {
            int index = replicationNameIndex.getAndIncrement();
            replicationNames[index] = ack.replicationName;
            if (index == size - 1) {
                // 第二步: 分析锁冲突(因为是基于轻量级锁来实现复制的，锁冲突也就意味着复制发生了冲突)
                // 1. 如果客户端C拿到的行锁数>=2，保留C，撤销其他的
                // 2. 如果没有一个客户端拿到的行锁数>=2，那么按复制名的自然序保留排在最前面的那个，撤销之后的
                int quorum = size / 2 + 1;
                String candidateReplicationName = null;
                TreeMap<String, AtomicInteger> map = new TreeMap<>();
                for (String rn : replicationNames) {
                    AtomicInteger count = map.get(rn);
                    if (count == null) {
                        count = new AtomicInteger(0);
                        map.put(rn, count);
                    }
                    if (count.incrementAndGet() >= quorum)
                        candidateReplicationName = rn;
                }
                if (candidateReplicationName == null)
                    candidateReplicationName = map.firstKey();

                // 第三步: 处理锁冲突
                for (int i = 2, length = names.length; i < length; i++) {
                    String name = names[i];
                    if (!local[i]) {
                        ReplicationCheckConflict packet = new ReplicationCheckConflict(mapName, key, replicationName);
                        session.sendAsync(packet, name, null);
                    }
                }
            }
        };

        // 从2开始，前两个不是节点名
        for (int i = 2, length = names.length; i < length; i++) {
            String name = names[i];
            if (name.indexOf(':') == -1) {
                name += ":" + Constants.DEFAULT_TCP_PORT;
                names[i] = name;
            }
            if (localHostAndPort.equals(NetNode.createTCP(name))) {
                replicationNames[replicationNameIndex.getAndIncrement()] = replicationName;
                local[i] = true;
            } else {
                ReplicationCheckConflict packet = new ReplicationCheckConflict(mapName, key, replicationName);
                session.sendAsync(packet, name, handler);
            }
        }
        return null;
    }
}
