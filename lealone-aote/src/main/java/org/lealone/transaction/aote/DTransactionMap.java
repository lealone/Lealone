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

import org.lealone.db.session.Session;
import org.lealone.storage.DistributedStorageMap;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;

public class DTransactionMap<K, V> extends AOTransactionMap<K, V> {

    private final DistributedStorageMap<K, TransactionalValue> map;
    private final Session session;
    private final StorageDataType valueType;

    DTransactionMap(AOTransaction transaction, StorageMap<K, TransactionalValue> map) {
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

    @Override
    public void addIfAbsent(K key, V value, Transaction.Listener listener) {
        map.replicationPut(session, key, value, valueType);
        listener.operationComplete();
    }

    @Override
    public DTransactionMap<K, V> getInstance(Transaction transaction) {
        return new DTransactionMap<>((AOTransaction) transaction, map);
    }
}
