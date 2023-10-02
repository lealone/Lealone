/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import org.lealone.db.async.Future;
import org.lealone.storage.CursorParameters;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

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

    public Future<Integer> addIfAbsent(K key, V value);

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

    public int tryRemove(K key, Object oldTValue, boolean isLockedBySelf);

    public default int tryLock(K key, Object oldTValue) {
        return tryLock(key, oldTValue, null);
    }

    public int tryLock(K key, Object oldTValue, int[] columnIndexes);

    public boolean isLocked(Object oldTValue, int[] columnIndexes);

    public Object getTransactionalValue(K key);

    public StorageDataType getTransactionalValueType();

    public int addWaitingTransaction(Object key, Object oldTValue);
}
