/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.lealone.db.async.Future;
import org.lealone.storage.IterationParameters;
import org.lealone.storage.StorageMap;

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

    /**
     * Update the value for the given key, without adding an undo log entry.
     *
     * @param key the key
     * @param value the value
     * @return the old value
     */
    public V putCommitted(K key, V value);

    /**
     * Iterate over entries.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public Iterator<TransactionMapEntry<K, V>> entryIterator(K from);

    public Iterator<TransactionMapEntry<K, V>> entryIterator(IterationParameters<K> parameters);

    /**
     * Iterate over keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public Iterator<K> keyIterator(K from);

    /**
     * Iterate over keys.
     *
     * @param from the first key to return
     * @param includeUncommitted whether uncommitted entries should be included
     * @return the iterator
     */
    public Iterator<K> keyIterator(K from, boolean includeUncommitted);

    public Future<Integer> addIfAbsent(K key, V value);

    public default int tryUpdate(K key, V newValue) {
        Object oldTransactionalValue = getTransactionalValue(key);
        return tryUpdate(key, newValue, null, oldTransactionalValue);
    }

    public default int tryUpdate(K key, V newValue, Object oldTransactionalValue) {
        return tryUpdate(key, newValue, null, oldTransactionalValue);
    }

    public default int tryUpdate(K key, V newValue, int[] columnIndexes) {
        Object oldTransactionalValue = getTransactionalValue(key);
        return tryUpdate(key, newValue, columnIndexes, oldTransactionalValue);
    }

    public default int tryUpdate(K key, V newValue, int[] columnIndexes, Object oldTransactionalValue) {
        return tryUpdate(key, newValue, columnIndexes, oldTransactionalValue, false);
    }

    public int tryUpdate(K key, V newValue, int[] columnIndexes, Object oldTransactionalValue, boolean isLockedBySelf);

    public default int tryRemove(K key) {
        Object oldTransactionalValue = getTransactionalValue(key);
        return tryRemove(key, oldTransactionalValue);
    }

    public default int tryRemove(K key, Object oldTransactionalValue) {
        return tryRemove(key, oldTransactionalValue, false);
    }

    public int tryRemove(K key, Object oldTransactionalValue, boolean isLockedBySelf);

    public default boolean tryLock(K key) {
        Object oldTransactionalValue = getTransactionalValue(key);
        return tryLock(key, oldTransactionalValue);
    }

    public default boolean tryLock(K key, Object oldTransactionalValue) {
        return tryLock(key, oldTransactionalValue, false, null);
    }

    public boolean tryLock(K key, Object oldTransactionalValue, boolean addToWaitingQueue, int[] columnIndexes);

    public boolean isLocked(Object oldTransactionalValue, int[] columnIndexes);

    public Object[] getValueAndRef(K key, int[] columnIndexes);

    public Object getValue(Object oldTransactionalValue);

    public Object getTransactionalValue(K key);

    public int addWaitingTransaction(Object key, Object oldTransactionalValue, Transaction.Listener listener);

    public default String checkReplicationConflict(ByteBuffer key, String replicationName) {
        return null;
    }
}
