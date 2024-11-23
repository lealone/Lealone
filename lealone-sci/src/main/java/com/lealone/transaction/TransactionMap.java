/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction;

import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.lock.Lockable;
import com.lealone.storage.CursorParameters;
import com.lealone.storage.StorageMap;

public interface TransactionMap<K, V> extends StorageMap<K, V> {

    // 以下三个api只是改变返回类型

    @Override
    public default TransactionMapCursor<K, V> cursor(K from) {
        return cursor(CursorParameters.create(from));
    }

    @Override
    public default TransactionMapCursor<K, V> cursor() {
        return cursor(CursorParameters.create(null));
    }

    @Override
    public TransactionMapCursor<K, V> cursor(CursorParameters<K> parameters);

    /**
     * Get the raw map.
     *
     * @return the raw map
     */
    public StorageMap<?, ?> getRawMap();

    /**
     * Get the size of the raw map. This includes uncommitted entries, and
     * transiently removed entries, so it is the maximum number of entries.
     *
     * @return the maximum size
     */
    public long getRawSize();

    /**
     * Get a clone of this map for the given transaction.
     *
     * @param transaction the transaction
     * @return the map
     */
    public TransactionMap<K, V> getInstance(Transaction transaction);

    public void addIfAbsent(K key, Lockable lockable, AsyncResultHandler<Integer> handler);

    // 若是定义成append(lockable,handler)，java的泛型会识别为append(V value,handler)
    public void append(AsyncResultHandler<K> handler, Lockable lockable);

    public default int tryUpdate(K key, V newValue) {
        Lockable lockable = getLockableValue(key);
        return tryUpdate(key, newValue, lockable, false);
    }

    public int tryUpdate(K key, V newValue, Lockable lockable, boolean isLockedBySelf);

    public default int tryRemove(K key) {
        Lockable lockable = getLockableValue(key);
        return tryRemove(key, lockable, false);
    }

    public int tryRemove(K key, Lockable lockable, boolean isLockedBySelf);

    public int tryLock(Lockable lockable);

    public boolean isLocked(Lockable lockable);

    public int addWaitingTransaction(Lockable lockable);

    public Lockable getLockableValue(K key);

}
