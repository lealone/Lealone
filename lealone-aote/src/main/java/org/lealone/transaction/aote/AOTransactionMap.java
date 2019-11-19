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

import org.lealone.db.Session;
import org.lealone.net.NetNode;
import org.lealone.storage.DistributedStorageMap;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;

//支持分布式场景(包括replication和sharding)
public class AOTransactionMap<K, V> extends AMTransactionMap<K, V> {

    private final AOTransaction transaction;
    private final DistributedStorageMap<K, TransactionalValue> map;
    private final Session session;
    private final StorageDataType valueType;

    AOTransactionMap(AOTransaction transaction, StorageMap<K, TransactionalValue> map) {
        super(transaction, map);
        this.transaction = transaction;
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

    @Override
    public void addIfAbsent(K key, V value, Transaction.Listener listener) {
        map.replicationPut(session, key, value, valueType);
        listener.operationComplete();
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
            else if (DTRValidator.containsReplication(data.getGlobalReplicationName())) {
                boolean isValid = DTRValidator.validateReplication(data.getGlobalReplicationName(),
                        transaction.validator);
                if (isValid) {
                    DTRValidator.removeReplication(data.getGlobalReplicationName());
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

    @Override
    protected int tryUpdateOrRemove(K key, V value, int[] columnIndexes, TransactionalValue oldTransactionalValue) {
        long tid = oldTransactionalValue.getTid();
        if (tid != 0 && tid != transaction.transactionId && tid % 2 == 1) {
            boolean isValid = transaction.transactionEngine.validateTransaction(tid, transaction);
            if (isValid) {
                transaction.commitAfterValidate(tid);
            } else {
                return Transaction.OPERATION_NEED_WAIT;
            }
        }
        int ret = super.tryUpdateOrRemove(key, value, columnIndexes, oldTransactionalValue);
        if (ret == Transaction.OPERATION_COMPLETE) {
            oldTransactionalValue.incrementVersion();
            if (transaction.globalTransactionName != null)
                DTRValidator.addReplication(transaction.globalTransactionName);
        }
        return ret;
    }

    @Override
    public AMTransactionMap<K, V> getInstance(Transaction transaction) {
        return new AOTransactionMap<>((AOTransaction) transaction, map);
    }
}
