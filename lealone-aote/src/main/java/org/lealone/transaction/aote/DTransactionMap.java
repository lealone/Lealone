/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;

import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.db.session.Session;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.aote.tvalue.TransactionalValue;

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
        return (V) map.get(session, key).get();
    }

    @Override
    public Future<Integer> addIfAbsent(K key, V value) {
        AsyncCallback<Integer> ac = new AsyncCallback<>();
        map.put(session, key, value, valueType, true).onSuccess(r -> {
            ByteBuffer resultByteBuffer = (ByteBuffer) r;
            if (resultByteBuffer.get() == 1)
                ac.setAsyncResult(Transaction.OPERATION_COMPLETE);
            else
                ac.setAsyncResult((Throwable) null);
        }).onFailure(t -> {
            ac.setAsyncResult(t);
        });
        return ac;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K append(V value, AsyncHandler<AsyncResult<K>> topHandler) {
        map.append(session, value, valueType).onSuccess(r -> {
            topHandler.handle(new AsyncResult<>((K) r));
        }).onFailure(t -> {
            topHandler.handle(new AsyncResult<>(t));
        });
        return null;
    }

    @Override
    public DTransactionMap<K, V> getInstance(Transaction transaction) {
        return new DTransactionMap<>((AOTransaction) transaction, map);
    }
}
