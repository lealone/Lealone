/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.Constants;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;
import org.lealone.db.session.Session;
import org.lealone.net.NetNode;
import org.lealone.server.protocol.AckPacketHandler;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.dt.DTransactionValidate;
import org.lealone.server.protocol.dt.DTransactionValidateAck;
import org.lealone.server.protocol.replication.ReplicationCheckConflict;
import org.lealone.server.protocol.replication.ReplicationCheckConflictAck;
import org.lealone.server.protocol.replication.ReplicationHandleConflict;
import org.lealone.storage.replication.ConsistencyLevel;

//效验分布式事务和复制是否成功
//DT表示Distributed Transaction，R表示Replication
class DTRValidator {

    private static final DTStatusCache cache = new DTStatusCache();

    // key: transactionName, value: [ globalTransactionName, commitTimestamp ].
    private static final ConcurrentHashMap<String, Object[]> transactions = new ConcurrentHashMap<>();

    // key: replicationName, value: replicationName.
    private static final ConcurrentHashMap<String, String> replications = new ConcurrentHashMap<>();

    static void addTransaction(AOTransaction transaction, String globalTransactionName, long commitTimestamp) {
        Object[] v = { globalTransactionName, commitTimestamp };
        transactions.put(transaction.transactionName, v);
        transactions.put(globalTransactionName, v);
    }

    static void removeTransaction(AOTransaction transaction, String globalTransactionName) {
        transactions.remove(transaction.transactionName);
        transactions.remove(globalTransactionName);
    }

    static boolean validateTransaction(String globalTransactionName) {
        if (globalTransactionName.startsWith("replication:"))
            return replications.containsKey(globalTransactionName.substring("replication:".length()));
        else
            return transactions.containsKey(globalTransactionName);
    }

    /**
     * 检查事务是否有效
     * 
     * @param oldTid 要检查的行存入数据库的旧事务id
     * @param currentTransaction 当前事务
     * @return true 有效 
     */
    static boolean validateTransaction(long oldTid, AOTransaction currentTransaction) {
        long commitTimestamp = cache.get(oldTid);
        // 1.上一次已经查过了，已确认过是条无效的记录
        if (commitTimestamp == -2)
            return false;
        // 2. 是有效的事务记录，再进一步判断是否小于等于当前事务的开始时间戳
        if (commitTimestamp != -1)
            return commitTimestamp <= currentTransaction.transactionId;

        String localHostAndPort = NetNode.getLocalTcpHostAndPort();
        String localTransactionName = AOTransaction.getTransactionName(localHostAndPort, oldTid);

        Object[] v = transactions.get(localTransactionName);
        if (v == null) // TODO
            return true;

        commitTimestamp = (long) v[1];
        String globalTransactionName = (String) v[0];
        String[] a = globalTransactionName.split(",");
        boolean isFullSuccessful = true;

        // 第一个是协调者的本地事务名，其他是参与者的hostAndPort
        for (int i = 1; i < a.length; i++) {
            String hostAndPort = a[i].trim();
            if (!localHostAndPort.equals(hostAndPort)) {
                if (!validateRemoteTransaction(hostAndPort, globalTransactionName, currentTransaction.getSession())) {
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

    private static boolean validateRemoteTransaction(String hostAndPort, String globalTransactionName,
            Session session) {
        DTransactionValidate packet = new DTransactionValidate(globalTransactionName);
        Future<DTransactionValidateAck> ack = session.send(packet, hostAndPort);
        return ack.get().isValid;
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

    static Future<String> handleReplicationConflict(String mapName, ByteBuffer key, String replicationName,
            Session session, String oldReplicationName) {
        // 第一步: 获取各节点的锁占用情况
        String[] names = replicationName.split(",");
        boolean[] local = new boolean[names.length];
        NetNode localHostAndPort = NetNode.getLocalTcpNode();
        int replicationNodeSize = names.length - 2;
        String[] replicationNames = new String[replicationNodeSize];

        AtomicInteger ackCount = new AtomicInteger();
        AsyncCallback<String> asyncCallback = new AsyncCallback<>();
        int index = 0;

        // 从2开始，前两个不是节点名
        for (int i = 2, length = names.length; i < length; i++) {
            String name = names[i];
            if (name.indexOf(':') == -1) {
                name += ":" + Constants.DEFAULT_TCP_PORT;
                names[i] = name;
            }
            if (localHostAndPort.equals(NetNode.createTCP(name))) {
                // 这里是当前节点，因为被上一个未提交的事务提前拿到锁了，所以就用oldReplicationName
                replicationNames[index++] = oldReplicationName;
                local[i] = true;
                ackCount.incrementAndGet();
            } else {
                final int replicationNameIndex = index++;
                // 这个handler不能提到for循环外，放在循环里面才会得到一个新实例，
                // 这样通过replicationNameIndex给replicationNames赋值时才正确。
                AckPacketHandler<Void, ReplicationCheckConflictAck> handler = ack -> {
                    replicationNames[replicationNameIndex] = ack.replicationName;
                    if (ackCount.incrementAndGet() == replicationNodeSize) {
                        // 第二步: 分析锁冲突(因为是基于轻量级锁来实现复制的，锁冲突也就意味着复制发生了冲突)
                        // 1. 如果客户端C拿到的行锁数>=2，保留C，撤销其他的
                        // 2. 如果没有一个客户端拿到的行锁数>=2，那么按复制名的自然序保留排在最前面的那个，撤销之后的
                        int quorum = replicationNodeSize / 2 + 1;
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

                        asyncCallback.setAsyncResult(candidateReplicationName);

                        // 第三步: 处理锁冲突
                        for (int j = 2, len = names.length; j < len; j++) {
                            String n = names[j];
                            // 只发给跟candidateReplicationName不同的节点
                            if (!local[j] && !replicationNames[replicationNameIndex].equals(candidateReplicationName)) {
                                Packet p = new ReplicationHandleConflict(mapName, key.slice(), replicationName);
                                session.send(p, n);
                            }
                        }
                    }
                    return null;
                };
                // 需要用key.slice()，不能直接传key，在一个循环中多次读取ByteBuffer会产生BufferUnderflowException
                Packet packet = new ReplicationCheckConflict(mapName, key.slice(), replicationName);
                session.send(packet, name, handler);
            }
        }
        return asyncCallback;
    }
}
