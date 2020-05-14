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

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.Session;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;

public class DTransactionMap<K, V> extends AOTransactionMap<K, V> {

    private final Session session;
    private final StorageDataType valueType;

    DTransactionMap(AOTransaction transaction, StorageMap<K, TransactionalValue> map) {
        super(transaction, map);
        session = transaction.getSession();
        valueType = getValueType();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        return (V) map.get(session, key);
    }

    @Override
    public void addIfAbsent(K key, V value, Transaction.Listener listener) {
        map.put(session, key, value, valueType, true).onSuccess(r -> {
            ByteBuffer resultByteBuffer = (ByteBuffer) r;
            if (resultByteBuffer.get() == 1)
                listener.operationComplete();
            else
                listener.operationUndo();
        }).onFailure(t -> {
            listener.setException(new RuntimeException(t));
            listener.operationUndo();
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public void append(V value, Transaction.Listener listener, AsyncHandler<AsyncResult<K>> topHandler) {
        map.append(session, value, valueType).onSuccess(r -> {
            listener.operationComplete();
            topHandler.handle(new AsyncResult<>((K) r));
        }).onFailure(t -> {
            listener.setException(new RuntimeException(t));
            listener.operationUndo();
        });
    }

    @Override
    public DTransactionMap<K, V> getInstance(Transaction transaction) {
        return new DTransactionMap<>((AOTransaction) transaction, map);
    }
}
