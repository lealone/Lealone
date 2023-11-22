/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.Future;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.storage.CursorParameters;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapProxy;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.TransactionMapCursor;

public class AOTransactionMapProxy<K, V> extends StorageMapProxy<K, V> implements TransactionMap<K, V> {

    private final AOTransactionMap<K, V> map;

    public AOTransactionMapProxy(AOTransactionMap<K, V> map, Scheduler scheduler) {
        super(map, scheduler);
        this.map = map;
    }

    @Override
    public StorageMap<?, ?> getRawMap() {
        return map.getRawMap();
    }

    @Override
    public long getRawSize() {
        return map.getRawSize();
    }

    @Override
    public TransactionMap<K, V> getInstance(Transaction transaction) {
        return map.getInstance(transaction);
    }

    @Override
    public TransactionMapCursor<K, V> cursor(K from) {
        return map.cursor(from);
    }

    @Override
    public TransactionMapCursor<K, V> cursor() {
        return map.cursor();
    }

    @Override
    public TransactionMapCursor<K, V> cursor(CursorParameters<K> parameters) {
        return map.cursor(parameters);
    }

    @Override
    public Future<Integer> addIfAbsent(K key, V value) {
        return map.addIfAbsent(key, value);
    }

    @Override
    public int tryUpdate(K key, V newValue) {
        return map.tryUpdate(key, newValue);
    }

    @Override
    public int tryUpdate(K key, V newValue, Object oldTValue) {
        return map.tryUpdate(key, newValue, oldTValue);
    }

    @Override
    public int tryUpdate(K key, V newValue, int[] columnIndexes) {
        return map.tryUpdate(key, newValue, columnIndexes);
    }

    @Override
    public int tryUpdate(K key, V newValue, int[] columnIndexes, Object oldTValue) {
        return map.tryUpdate(key, newValue, columnIndexes, oldTValue);
    }

    @Override
    public int tryUpdate(K key, V newValue, int[] columnIndexes, Object oldTValue,
            boolean isLockedBySelf) {
        return map.tryUpdate(key, newValue, columnIndexes, oldTValue, isLockedBySelf);
    }

    @Override
    public int tryRemove(K key) {
        return map.tryRemove(key);
    }

    @Override
    public int tryRemove(K key, Object oldTValue) {
        return map.tryRemove(key, oldTValue);
    }

    @Override
    public int tryRemove(K key, Object oldTValue, boolean isLockedBySelf) {
        return map.tryRemove(key, oldTValue, isLockedBySelf);
    }

    @Override
    public int tryLock(K key, Object oldTValue) {
        return map.tryLock(key, oldTValue);
    }

    @Override
    public int tryLock(K key, Object oldTValue, int[] columnIndexes) {
        return map.tryLock(key, oldTValue, columnIndexes);
    }

    @Override
    public boolean isLocked(Object oldTValue, int[] columnIndexes) {
        return map.isLocked(oldTValue, columnIndexes);
    }

    @Override
    public Object getTransactionalValue(K key) {
        return map.getTransactionalValue(key);
    }

    @Override
    public StorageDataType getTransactionalValueType() {
        return map.getTransactionalValueType();
    }

    @Override
    public int addWaitingTransaction(Object key, Object oldTValue) {
        return map.addWaitingTransaction(key, oldTValue);
    }

    @Override
    public void submitTask(AsyncTask task) {
        map.getTransaction().submitTask(task);
    }
}
