/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction;

import com.lealone.db.async.Future;
import com.lealone.storage.CursorParameters;
import com.lealone.storage.StorageMap;
import com.lealone.storage.type.StorageDataType;

public interface TransactionMap<K, V> extends StorageMap<K, V> {

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

    public default Future<Integer> addIfAbsent(K key, V value) {
        return addIfAbsent(key, value, true);
    }

    public Future<Integer> addIfAbsent(K key, V value, boolean writeRedoLog);

    public default int tryUpdate(K key, V newValue) {
        Object oldTValue = getTransactionalValue(key);
        return tryUpdate(key, newValue, null, oldTValue);
    }

    public default int tryUpdate(K key, V newValue, Object oldTValue) {
        return tryUpdate(key, newValue, null, oldTValue);
    }

    public default int tryUpdate(K key, V newValue, int[] columnIndexes) {
        Object oldTValue = getTransactionalValue(key);
        return tryUpdate(key, newValue, columnIndexes, oldTValue);
    }

    public default int tryUpdate(K key, V newValue, int[] columnIndexes, Object oldTValue) {
        return tryUpdate(key, newValue, columnIndexes, oldTValue, false);
    }

    public int tryUpdate(K key, V newValue, int[] columnIndexes, Object oldTValue,
            boolean isLockedBySelf);

    public default int tryRemove(K key) {
        Object oldTValue = getTransactionalValue(key);
        return tryRemove(key, oldTValue);
    }

    public default int tryRemove(K key, Object oldTValue) {
        return tryRemove(key, oldTValue, false);
    }

    public default int tryRemove(K key, Object oldTValue, boolean isLockedBySelf) {
        return tryRemove(key, oldTValue, isLockedBySelf, true);
    }

    public int tryRemove(K key, Object oldTValue, boolean isLockedBySelf, boolean writeRedoLog);

    public default int tryLock(K key, Object oldTValue) {
        return tryLock(key, oldTValue, null);
    }

    public int tryLock(K key, Object oldTValue, int[] columnIndexes);

    public boolean isLocked(Object oldTValue, int[] columnIndexes);

    public Object getTransactionalValue(K key);

    public StorageDataType getTransactionalValueType();

    public int addWaitingTransaction(Object key, Object oldTValue);
}
