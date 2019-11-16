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

import java.util.concurrent.ConcurrentHashMap;

import org.lealone.db.Constants;
import org.lealone.db.Session;
import org.lealone.net.NetNode;
import org.lealone.storage.DistributedStorageMap;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.aote.AMTransactionMap;
import org.lealone.transaction.aote.TransactionalValue;

public class AOTransactionMap<K, V> extends AMTransactionMap<K, V> {

    static class AOReplicationMap<K, V> extends AOTransactionMap<K, V> {

        private final DistributedStorageMap<K, TransactionalValue> map;
        private final Session session;
        private final StorageDataType valueType;

        AOReplicationMap(AOTransaction transaction, StorageMap<K, TransactionalValue> map) {
            super(transaction, map);
            this.map = (DistributedStorageMap<K, TransactionalValue>) map;
            session = transaction.getSession();
            valueType = getValueType();
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(K key) {
            return (V) map.replicationGet(session, key);
        }

        @SuppressWarnings("unchecked")
        @Override
        public V put(K key, V value) {
            return (V) map.replicationPut(session, key, value, valueType);
        }

        @SuppressWarnings("unchecked")
        @Override
        public K append(V value) {
            return (K) map.replicationAppend(session, value, valueType);
        }
    }

    static final ConcurrentHashMap<String, String> replication = new ConcurrentHashMap<>();

    private final AOTransaction transaction;

    AOTransactionMap(AOTransaction transaction, StorageMap<K, TransactionalValue> map) {
        super(transaction, map);
        this.transaction = transaction;
    }

    @Override
    protected TransactionalValue getDistributedValue(K key, TransactionalValue data) {
        // 第一种: 复制的场景
        // 数据从节点A迁移到节点B的过程中，如果把A中未提交的值也移到B中，
        // 那么在节点B中会读到不一致的数据，此时需要从节点A读出正确的值
        // TODO 如何更高效的判断，不用比较字符串
        if (data.getHostAndPort() != null && !data.getHostAndPort().equals(NetNode.getLocalTcpHostAndPort())) {
            return getRemoteTransactionalValue(data.getHostAndPort(), key);
        }
        // 第二种: 分布式场景
        long tid = data.getTid();
        if (tid % 2 == 1) {
            boolean isValid = transaction.transactionEngine.validateTransaction(tid, transaction);
            if (isValid) {
                transaction.commitAfterValidate(tid);
                return getValue(key, map.get(key));
            }
        } else if (data.getGlobalReplicationName() != null) {
            if (data.isReplicated())
                return data;
            else if (replication.containsKey(data.getGlobalReplicationName())) {
                boolean isValid = validateReplication(data.getGlobalReplicationName());
                if (isValid) {
                    replication.remove(data.getGlobalReplicationName());
                    data.setReplicated(true);
                    return data;
                }
            }
        }
        return null;
    }

    // TODO 还未实现
    private TransactionalValue getRemoteTransactionalValue(String hostAndPort, K key) {
        return null;
    }

    private boolean validateReplication(String globalTransactionName) {
        boolean isValid = true;
        String[] names = globalTransactionName.split(",");
        NetNode localHostAndPort = NetNode.getLocalTcpNode();
        // 从1开始，第一个不是节点名
        for (int i = 1, size = names.length; i < size && isValid; i++) {
            String name = names[i];
            if (name.indexOf(':') == -1) {
                name += ":" + Constants.DEFAULT_TCP_PORT;
            }

            if (localHostAndPort.equals(NetNode.createTCP(name)))
                continue;
            isValid = isValid && transaction.validator.validate(name, "replication:" + globalTransactionName);
        }
        return isValid;
    }

    public boolean trySet(K key, V value) {
        TransactionalValue oldValue = map.get(key);
        TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, oldValue,
                map.getValueType(), null);
        String mapName = getName();
        if (oldValue == null) {
            // a new value
            transaction.getUndoLog().add(mapName, key, oldValue, newValue);
            TransactionalValue old = map.putIfAbsent(key, newValue);
            if (old != null) {
                transaction.getUndoLog().undo();
                return false;
            }
            return true;
        }
        long tid = oldValue.getTid();
        if (tid == 0) {
            // committed
            transaction.getUndoLog().add(mapName, key, oldValue, newValue);
            // the transaction is committed:
            // overwrite the value
            if (!map.replace(key, oldValue, newValue)) {
                // somebody else was faster
                transaction.getUndoLog().undo();
                return false;
            }
            oldValue.incrementVersion();
            if (newValue.getGlobalReplicationName() != null)
                replication.put(newValue.getGlobalReplicationName(), newValue.getGlobalReplicationName());
            return true;
        }
        if (tid == transaction.transactionId) {
            // added or updated by this transaction
            transaction.getUndoLog().add(mapName, key, oldValue, newValue);
            if (!map.replace(key, oldValue, newValue)) {
                // strange, somebody overwrote the value
                // even though the change was not committed
                transaction.getUndoLog().undo();
                return false;
            }
            oldValue.incrementVersion();
            if (newValue.getGlobalReplicationName() != null)
                replication.put(newValue.getGlobalReplicationName(), newValue.getGlobalReplicationName());
            return true;
        }

        if (tid % 2 == 1) {
            boolean isValid = transaction.transactionEngine.validateTransaction(tid, transaction);
            if (isValid) {
                transaction.commitAfterValidate(tid);
                return trySet(key, value);
            }
        }
        // the transaction is not yet committed
        return false;
    }

    @Override
    public AOTransactionMap<K, V> getInstance(Transaction transaction) {
        AOTransaction t = (AOTransaction) transaction;
        if (t.isShardingMode())
            return new AOReplicationMap<>(t, map);
        else
            return new AOTransactionMap<>(t, map);
    }
}
