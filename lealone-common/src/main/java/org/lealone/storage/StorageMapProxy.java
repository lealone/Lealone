/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.util.concurrent.atomic.AtomicLong;

import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.Future;
import org.lealone.db.async.PendingTaskHandlerBase;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.db.session.Session;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.TransactionEngine;

public class StorageMapProxy<K, V> extends PendingTaskHandlerBase implements StorageMap<K, V> {

    private final StorageMap<K, V> map;

    public StorageMapProxy(StorageMap<K, V> map, Scheduler scheduler) {
        this.map = map;
        this.scheduler = scheduler;
    }

    @Override
    public String getName() {
        return map.getName();
    }

    @Override
    public StorageDataType getKeyType() {
        return map.getKeyType();
    }

    @Override
    public StorageDataType getValueType() {
        return map.getValueType();
    }

    @Override
    public Storage getStorage() {
        return map.getStorage();
    }

    @Override
    public void setMaxKey(K key) {
        map.setMaxKey(key);
    }

    @Override
    public long getAndAddKey(long delta) {
        return map.getAndAddKey(delta);
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return map.cursor(from);
    }

    @Override
    public StorageMapCursor<K, V> cursor() {
        return map.cursor();
    }

    @Override
    public void gc() {
        map.gc();
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public void get(K key, AsyncHandler<AsyncResult<V>> handler) {
        map.get(key, handler);
    }

    @Override
    public Object[] getObjects(K key, int[] columnIndexes) {
        return map.getObjects(key, columnIndexes);
    }

    @Override
    public Future<Object> get(Session session, K key) {
        return map.get(session, key);
    }

    @Override
    public K firstKey() {
        return map.firstKey();
    }

    @Override
    public K lastKey() {
        return map.lastKey();
    }

    @Override
    public K lowerKey(K key) {
        return map.lowerKey(key);
    }

    @Override
    public K floorKey(K key) {
        return map.floorKey(key);
    }

    @Override
    public K higherKey(K key) {
        return map.higherKey(key);
    }

    @Override
    public K ceilingKey(K key) {
        return map.ceilingKey(key);
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        return map.areValuesEqual(a, b);
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public void decrementSize() {
        map.decrementSize();
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean isInMemory() {
        return map.isInMemory();
    }

    @Override
    public StorageMapCursor<K, V> cursor(CursorParameters<K> parameters) {
        return map.cursor(parameters);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public void remove() {
        map.remove();
    }

    @Override
    public boolean isClosed() {
        return map.isClosed();
    }

    @Override
    public void close() {
        map.close();
    }

    @Override
    public void save() {
        map.save();
    }

    @Override
    public void save(long dirtyMemory) {
        map.save(dirtyMemory);
    }

    @Override
    public boolean needGc() {
        return map.needGc();
    }

    @Override
    public void gc(TransactionEngine te) {
        map.gc(te);
    }

    @Override
    public void fullGc(TransactionEngine te) {
        map.fullGc(te);
    }

    @Override
    public long collectDirtyMemory(TransactionEngine te, AtomicLong usedMemory) {
        return map.collectDirtyMemory(te, usedMemory);
    }

    @Override
    public void markDirty(Object key) {
        map.markDirty(key);
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return map.equals(o);
    }

    @Override
    public String toString() {
        return map.toString();
    }

    @Override
    public long getDiskSpaceUsed() {
        return map.getDiskSpaceUsed();
    }

    @Override
    public long getMemorySpaceUsed() {
        return map.getMemorySpaceUsed();
    }

    @Override
    public boolean hasUnsavedChanges() {
        return map.hasUnsavedChanges();
    }

    @Override
    public void repair() {
        AsyncCallback<Integer> ac = AsyncCallback.createConcurrentCallback();
        submitTask(() -> {
            map.repair();
            ac.setAsyncResult(1);
        });
        ac.get();
    }

    @Override
    public V put(K key, V value) {
        AsyncCallback<V> ac = AsyncCallback.createConcurrentCallback();
        submitTask(() -> {
            map.put(key, value, ar -> {
                if (ar.isSucceeded())
                    ac.setAsyncResult(ar.getResult());
                else
                    ac.setAsyncResult(ar.getCause());
            });
        });
        return ac.get();
    }

    @Override
    public void put(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        submitTask(() -> {
            map.put(key, value, handler);
        });
    }

    @Override
    public void put(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        submitTask(() -> {
            map.put(session, key, value, handler);
        });
    }

    @Override
    public V putIfAbsent(K key, V value) {
        AsyncCallback<V> ac = AsyncCallback.createConcurrentCallback();
        submitTask(() -> {
            V v = map.putIfAbsent(key, value);
            ac.setAsyncResult(v);
        });
        return ac.get();
    }

    @Override
    public void putIfAbsent(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        submitTask(() -> {
            map.putIfAbsent(key, value, handler);
        });
    }

    @Override
    public void putIfAbsent(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        submitTask(() -> {
            map.putIfAbsent(session, key, value, handler);
        });
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        AsyncCallback<Boolean> ac = AsyncCallback.createConcurrentCallback();
        submitTask(() -> {
            ac.setAsyncResult(map.replace(key, oldValue, newValue));
        });
        return ac.get();
    }

    @Override
    public void replace(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        submitTask(() -> {
            map.replace(key, oldValue, newValue, handler);
        });
    }

    @Override
    public void replace(Session session, K key, V oldValue, V newValue,
            AsyncHandler<AsyncResult<Boolean>> handler) {
        submitTask(() -> {
            map.replace(session, key, oldValue, newValue, handler);
        });
    }

    @Override
    public K append(V value) {
        AsyncCallback<K> ac = AsyncCallback.createConcurrentCallback();
        submitTask(() -> {
            K k = map.append(value);
            ac.setAsyncResult(k);
        });
        return ac.get();
    }

    @Override
    public void append(V value, AsyncHandler<AsyncResult<K>> handler) {
        submitTask(() -> {
            map.append(value, handler);
        });
    }

    @Override
    public void append(Session session, V value, AsyncHandler<AsyncResult<K>> handler) {
        submitTask(() -> {
            map.append(session, value, handler);
        });
    }

    @Override
    public V remove(K key) {
        AsyncCallback<V> ac = AsyncCallback.createConcurrentCallback();
        submitTask(() -> {
            V v = map.remove(key);
            ac.setAsyncResult(v);
        });
        return ac.get();
    }

    @Override
    public void remove(K key, AsyncHandler<AsyncResult<V>> handler) {
        submitTask(() -> {
            map.remove(key, handler);
        });
    }

    @Override
    public void remove(Session session, K key, AsyncHandler<AsyncResult<V>> handler) {
        submitTask(() -> {
            map.remove(session, key, handler);
        });
    }

    @Override
    public void submitTask(AsyncTask task) {
        if (scheduler == null) {
            task.run();
        } else {
            super.submitTask(task);
        }
    }
}
