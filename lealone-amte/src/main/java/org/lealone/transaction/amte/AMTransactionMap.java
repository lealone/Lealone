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
package org.lealone.transaction.amte;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.Session;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.DelegatedStorageMap;
import org.lealone.storage.IterationParameters;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;

public class AMTransactionMap<K, V> extends DelegatedStorageMap<K, V> implements TransactionMap<K, V> {

    static class AMReplicationMap<K, V> extends AMTransactionMap<K, V> {

        private final Session session;
        private final StorageDataType valueType;

        AMReplicationMap(AMTransaction transaction, StorageMap<K, TransactionalValue> map) {
            super(transaction, map);
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
    }

    private final AMTransaction transaction;
    protected final StorageMap<K, TransactionalValue> map;

    @SuppressWarnings("unchecked")
    public AMTransactionMap(AMTransaction transaction, StorageMap<K, TransactionalValue> map) {
        super((StorageMap<K, V>) map);
        this.transaction = transaction;
        this.map = map;
    }

    @Override
    public StorageDataType getValueType() {
        return ((TransactionalValueType) map.getValueType()).valueType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        TransactionalValue data = map.get(key);
        data = getValue(key, data);
        return data == null ? null : (V) data.getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key, int[] columnIndexes) {
        TransactionalValue data = map.get(key, columnIndexes);
        data = getValue(key, data);
        return data == null ? null : (V) data.getValue();
    }

    @Override
    public Object getTransactionalValue(K key) {
        return map.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object[] getUncommitted(K key, int[] columnIndexes) {
        TransactionalValue data = map.get(key, columnIndexes);
        data = getValue(key, data);
        return new Object[] { data == null ? null : (V) data.getValue(), data };
    }

    @Override
    public boolean isLocked(Object oldValue, int[] columnIndexes) {
        return ((TransactionalValue) oldValue).isLocked(transaction.transactionId, columnIndexes);
    }

    // 获得当前事务能看到的值，依据不同的隔离级别看到的值是不一样的
    protected TransactionalValue getValue(K key, TransactionalValue data) {
        if (data == null || data.getRefValue() == null) {
            // doesn't exist or deleted by a committed transaction
            return null;
        }
        TransactionalValue tv = data.getCommitted(transaction);
        if (tv != null)
            return tv;

        // 数据从节点A迁移到节点B的过程中，如果把A中未提交的值也移到B中，
        // 那么在节点B中会读到不一致的数据，此时需要从节点A读出正确的值
        // TODO 如何更高效的判断，不用比较字符串
        if (data.getHostAndPort() != null && !data.getHostAndPort().equals(NetEndpoint.getLocalTcpHostAndPort())) {
            return getRemoteTransactionalValue(data.getHostAndPort(), key);
        }
        long tid = data.getTid();
        tv = getValue(key, data, tid);
        if (tv != null)
            return tv;

        // 底层存储写入了未提交事务的脏数据，并且在事务提交前数据库崩溃了
        if (!transaction.transactionEngine.containsTransaction(tid)) {
            return data.undo(map, key);
        }

        return null;
    }

    protected TransactionalValue getValue(K key, TransactionalValue data, long tid) {
        return null;
    }

    protected TransactionalValue getRemoteTransactionalValue(String hostAndPort, K key) {
        return null;
    }

    /**
     * Update the value for the given key.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @param value the new value (not null)
     * @return the old value
     * @throws IllegalStateException if a lock timeout occurs
     */
    @Override
    public V put(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        return set(key, value);
    }

    @Override
    public boolean put(K key, V value, Transaction.Listener listener) {
        check(value);
        return trySetAsync(key, value, null, null, listener);
    }

    private void check(V value) {
        transaction.checkNotClosed();
        DataUtils.checkArgument(value != null, "The value may not be null");
    }

    @Override // 比put方法更高效，不需要返回值，所以也不需要事先调用get
    public void addIfAbsent(K key, V value, Transaction.Listener listener) {
        check(value);
        TransactionalValue ref = TransactionalValue.createRef();
        TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, null, map.getValueType(),
                null, ref);
        ref.setRefValue(newValue);
        String mapName = getName();
        final TransactionalLogRecord r = transaction.log(mapName, key, null, newValue);
        AsyncHandler<AsyncResult<TransactionalValue>> handler = (ar) -> {
            if (ar.isSucceeded()) {
                TransactionalValue old = ar.getResult();
                if (old != null) {
                    // 不能用logUndo()，因为它不是线程安全的，
                    // 在logUndo()中执行removeLast()在逻辑上也是不对的，
                    // 因为这里的异步回调函数可能是在不同线程中执行的，顺序也没有保证。
                    // transaction.logUndo();
                    r.undone = true;
                    listener.operationUndo();
                } else {
                    listener.operationComplete();
                }
            } else {
                r.undone = true;
                listener.operationUndo();
            }
        };
        map.putIfAbsent(key, ref, handler);
    }

    @SuppressWarnings("unchecked")
    private V set(K key, V value) {
        transaction.checkNotClosed();
        TransactionalValue oldValue = map.get(key);
        TransactionalValue retValue = null;
        if (oldValue != null) {
            retValue = getValue(key, oldValue);
        }
        // trySet可能会改变oldValue的内部状态，所以上一步提前拿到返回值
        boolean ok = trySet(key, value, oldValue, null);
        if (ok) {
            // oldValue = getValue(key, oldValue);
            return retValue == null ? null : (V) retValue.getValue();
        }
        throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED, "Entry is locked");
    }

    public boolean trySet(K key, V value) {
        TransactionalValue oldValue = map.get(key);
        return trySet(key, value, oldValue, null);
    }

    private boolean trySet(K key, V value, TransactionalValue oldValue, int[] columnIndexes) {
        CountDownLatch latch = new CountDownLatch(1);
        Transaction.Listener listener = new Transaction.Listener() {
            @Override
            public void operationUndo() {
                latch.countDown();
            }

            @Override
            public void operationComplete() {
                latch.countDown();
            }
        };

        trySetAsync(key, value, oldValue, null, listener);
        try {
            latch.await();
        } catch (InterruptedException e) {
            DbException.convert(e);
        }
        return true;
    }

    @SuppressWarnings("unused")
    private boolean trySetOld(K key, V value, TransactionalValue oldValue, int[] columnIndexes) {
        String mapName = getName();
        if (oldValue == null) {
            TransactionalValue ref = TransactionalValue.createRef(null);
            TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, null,
                    map.getValueType(), columnIndexes, ref);
            ref.setRefValue(newValue);
            // a new value
            transaction.log(mapName, key, oldValue, newValue);
            TransactionalValue old = map.putIfAbsent(key, ref);
            if (old != null) {
                transaction.logUndo();
                return false;
            }
            return true;
        }
        long tid = oldValue.getTid();
        if (tid == 0 || tid == transaction.transactionId
                || !oldValue.isLocked(transaction.transactionId, columnIndexes)) {
            TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, oldValue,
                    map.getValueType(), columnIndexes);
            // committed
            transaction.log(mapName, key, oldValue, newValue);
            TransactionalValue current = oldValue;
            // the transaction is committed:
            // overwrite the value
            while (!map.replace(key, current, newValue)) {
                current = map.get(key);
                if (!current.isLocked(transaction.transactionId, columnIndexes)) {
                    transaction.logUndo();
                    newValue = TransactionalValue.createUncommitted(transaction, value, current, map.getValueType(),
                            columnIndexes, oldValue);
                    // 只记录最初的oldValue，而不是current
                    transaction.log(mapName, key, oldValue, newValue);
                    continue;
                } else {
                    // somebody else was faster
                    transaction.logUndo();
                    return false;
                }
            }
            return true;
        }
        // if (tid == transaction.transactionId || !oldValue.isLocked(columnIndexes)) {
        // // added or updated by this transaction
        // transaction.log(mapName, key, oldValue, newValue);
        // if (!map.replace(key, oldValue, newValue)) {
        // // strange, somebody overwrote the value
        // // even though the change was not committed
        // transaction.logUndo();
        // return false;
        // }
        // return true;
        // }

        // the transaction is not yet committed
        return false;
    }

    // 返回true就表示要让出当前线程
    // 当value为null时代表delete
    // 当oldValue为null时代表insert
    // 当value和oldValue都不为null时代表update
    private boolean trySetAsync(K key, V value, TransactionalValue oldValue, int[] columnIndexes,
            Transaction.Listener listener) {
        String mapName = getName();
        // insert
        if (oldValue == null) {
            TransactionalValue ref = TransactionalValue.createRef(null);
            TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, null,
                    map.getValueType(), columnIndexes, ref);
            ref.setRefValue(newValue);
            transaction.log(mapName, key, null, newValue);
            AsyncHandler<AsyncResult<TransactionalValue>> handler = (ar) -> {
                if (ar.isSucceeded()) {
                    TransactionalValue old = ar.getResult();
                    if (old != null) {
                        transaction.logUndo();
                        listener.operationUndo();
                    } else {
                        listener.operationComplete();
                    }
                } else {
                    transaction.logUndo();
                    listener.operationUndo();
                }
            };
            map.putIfAbsent(key, ref, handler);
            return true;
        }

        // update or delete
        // 不同事务更新不同字段时，在循环里重试是可以的
        while (!oldValue.isLocked(transaction.transactionId, columnIndexes)) {
            TransactionalValue refValue = oldValue.getRefValue();
            TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, refValue,
                    map.getValueType(), columnIndexes, oldValue);
            transaction.log(mapName, key, refValue, newValue);
            if (oldValue.compareAndSet(refValue, newValue)) {
                listener.operationComplete();
                return false;
            } else {
                transaction.logUndo();
                listener.operationUndo();
                if (value == null) {
                    // 两个事务同时删除某一行时，因为删除操作是排它的，
                    // 所以只要一个compareAndSet成功了，另一个就没必要重试了
                    return true;
                }
            }
        }
        listener.operationUndo();
        // the transaction is not yet committed
        return true;
    }

    @Override
    public boolean tryPut(K key, Object oldValue, V newValue, int[] columnIndexes) {
        return tryPutOrRemove(key, newValue, (TransactionalValue) oldValue, columnIndexes);
    }

    @Override
    public boolean tryRemove(K key, Object oldValue) {
        return tryPutOrRemove(key, null, (TransactionalValue) oldValue, null);
    }

    // 在SQL层对应update或delete语句，如果当前行已经被其他事务锁住了那么返回true，当前事务要让出当前线程。
    // 当value为null时代表delete
    // 当value和oldValue都不为null时代表update
    private boolean tryPutOrRemove(K key, V value, TransactionalValue oldValue, int[] columnIndexes) {
        transaction.checkNotClosed();
        String mapName = getName();
        // 不同事务更新不同字段时，在循环里重试是可以的
        while (!oldValue.isLocked(transaction.transactionId, columnIndexes)) {
            TransactionalValue refValue = oldValue.getRefValue();
            TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, refValue,
                    map.getValueType(), columnIndexes, oldValue);
            transaction.log(mapName, key, refValue, newValue);
            if (oldValue.compareAndSet(refValue, newValue)) {
                return false;
            } else {
                transaction.logUndo();
                if (value == null) {
                    // 两个事务同时删除某一行时，因为删除操作是排它的，
                    // 所以只要一个compareAndSet成功了，另一个就没必要重试了
                    return true;
                }
            }
        }
        // 当前行已经被其他事务锁住了
        return true;
    }

    @Override
    public boolean tryLock(K key, Object oldValue) {
        TransactionalValue v = (TransactionalValue) oldValue;
        if (v.isLocked(transaction.transactionId, null))
            return false;

        TransactionalValue refValue = v.getRefValue();
        TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, refValue.getValue(), refValue,
                map.getValueType(), null, v);
        transaction.log(getName(), key, refValue, newValue, true);
        if (v.compareAndSet(refValue, newValue)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean put(K key, Object oldValue, V newValue, int[] columnIndexes, Transaction.Listener listener) {
        return trySetAsync(key, newValue, (TransactionalValue) oldValue, columnIndexes, listener);
    }

    @Override
    public boolean remove(K key, Object oldValue, Transaction.Listener listener) {
        return trySetAsync(key, null, (TransactionalValue) oldValue, null, listener);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V v = get(key);
        if (v == null)
            v = put(key, value);
        return v;
    }

    /**
     * Remove an entry.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @throws IllegalStateException if a lock timeout occurs
     */
    @Override
    public V remove(K key) {
        return set(key, null);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        V old = get(key);
        if (areValuesEqual(old, oldValue)) {
            put(key, newValue);
            return true;
        }
        return false;
    }

    @Override
    public K firstKey() {
        Iterator<K> it = keyIterator(null);
        return it.hasNext() ? it.next() : null;
    }

    @Override
    public K lastKey() {
        K k = map.lastKey();
        while (true) {
            if (k == null) {
                return null;
            }
            if (get(k) != null) {
                return k;
            }
            k = map.lowerKey(k);
        }
    }

    @Override
    public K lowerKey(K key) {
        while (true) {
            K k = map.lowerKey(key);
            if (k == null || get(k) != null) {
                return k;
            }
            key = k;
        }
    }

    @Override
    public K floorKey(K key) {
        while (true) {
            K k = map.floorKey(key);
            if (k == null || get(k) != null) {
                return k;
            }
            key = k;
        }
    }

    @Override
    public K higherKey(K key) {
        // TODO 处理事务
        // while (true) {
        // K k = map.higherKey(key);
        // if (k == null || get(k) != null) {
        // return k;
        // }
        // key = k;
        // }

        return map.higherKey(key);
    }

    @Override
    public K ceilingKey(K key) {
        while (true) {
            K k = map.ceilingKey(key);
            if (k == null || get(k) != null) {
                return k;
            }
            key = k;
        }
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        return map.areValuesEqual(a, b);
    }

    @Override
    public int size() {
        long size = sizeAsLong();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    /**
     * Get the size of the map as seen by this transaction.
     *
     * @return the size
     */
    @Override
    public long sizeAsLong() {
        long sizeRaw = map.sizeAsLong();
        long undoLogSize = 0;
        for (AMTransaction t : transaction.transactionEngine.getCurrentTransactions()) {
            undoLogSize += t.logRecords.size();
        }
        if (undoLogSize == 0) {
            return sizeRaw;
        }
        if (undoLogSize > sizeRaw) {
            // the undo log is larger than the map -
            // count the entries of the map
            long size = 0;
            StorageMapCursor<K, TransactionalValue> cursor = map.cursor();
            while (cursor.hasNext()) {
                K key = cursor.next();
                TransactionalValue data = cursor.getValue();
                data = getValue(key, data);
                if (data != null && data.getValue() != null) {
                    size++;
                }
            }
            return size;
        }
        // the undo log is smaller than the map -
        // scan the undo log and subtract invisible entries
        // re-fetch in case any transaction was committed now
        long size = map.sizeAsLong();
        String mapName = getName();
        Storage storage = map.getStorage();
        String tmpMapName = storage.nextTemporaryMapName();
        StorageMap<Object, Integer> temp = storage.openMap(tmpMapName, new ObjectDataType(), new ObjectDataType(),
                null);
        try {
            for (AMTransaction t : transaction.transactionEngine.getCurrentTransactions()) {
                LinkedList<TransactionalLogRecord> records = t.logRecords;
                for (TransactionalLogRecord r : records) {
                    String m = r.mapName;
                    if (!mapName.equals(m)) {
                        // a different map - ignore
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    K key = (K) r.key;
                    if (get(key) == null) {
                        Integer old = temp.put(key, 1);
                        // count each key only once (there might be multiple
                        // changes for the same key)
                        if (old == null) {
                            size--;
                        }
                    }
                }
            }
        } finally {
            temp.remove();
        }
        return size;
    }

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        return sizeAsLong() == 0;
    }

    @Override
    public boolean isInMemory() {
        return map.isInMemory();
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        final Iterator<Entry<K, V>> i = entryIterator(from);
        return new StorageMapCursor<K, V>() {
            Entry<K, V> e;

            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public K next() {
                e = i.next();
                return e.getKey();
            }

            @Override
            public void remove() {
                i.remove();
            }

            @Override
            public K getKey() {
                return e.getKey();
            }

            @Override
            public V getValue() {
                return e.getValue();
            }
        };
    }

    /**
     * Clear the map.
     */
    @Override
    public void clear() {
        // TODO truncate transactionally?
        map.clear();
    }

    @Override
    public void remove() {
        // 提前获取map名，一些存储引擎调用完 map.remove()后，再调用map.getName()会返回null
        String mapName = map.getName();
        if (mapName != null) {
            map.remove();
            transaction.transactionEngine.removeMap(mapName);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public K append(V value) { // 追加新记录时不会产生事务冲突
        TransactionalValue ref = TransactionalValue.createRef(null);
        TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, null, map.getValueType(),
                null, ref);
        ref.setRefValue(newValue);
        K key = map.append(ref);
        // 记事务log和append新值都是更新内存中的相应数据结构，所以不必把log调用放在append前面
        // 放在前面的话调用log方法时就不知道key是什么，当事务要rollback时就不知道如何修改map的内存数据
        transaction.logAppend((StorageMap<Object, TransactionalValue>) map, key, newValue);
        return key;
    }

    ///////////////////////// 以下是TransactionMap接口API的实现 /////////////////////////
    @Override
    public long rawSize() {
        return map.sizeAsLong();
    }

    @Override
    public AMTransactionMap<K, V> getInstance(Transaction transaction) {
        AMTransaction t = (AMTransaction) transaction;
        if (t.isShardingMode())
            return new AMReplicationMap<>(t, map);
        else
            return new AMTransactionMap<>(t, map);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V putCommitted(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        TransactionalValue newValue = TransactionalValue.createCommitted(value);
        TransactionalValue oldValue = map.put(key, newValue);
        return (V) (oldValue == null ? null : oldValue.getValue());
    }

    @Override
    public boolean isSameTransaction(K key) {
        TransactionalValue data = map.get(key);
        if (data == null) {
            // doesn't exist or deleted by a committed transaction
            return false;
        }

        return data.getTid() == transaction.transactionId;
    }

    @Override
    public Iterator<Entry<K, V>> entryIterator(K from) {
        return entryIterator(IterationParameters.create(from));
    }

    @Override
    public Iterator<Entry<K, V>> entryIterator(IterationParameters<K> parameters) {
        return new Iterator<Entry<K, V>>() {
            private Entry<K, V> current;
            private K currentKey = parameters.from;
            private StorageMapCursor<K, TransactionalValue> cursor = map.cursor(parameters);

            {
                fetchNext();
            }

            private void fetchNext() {
                while (cursor.hasNext()) {
                    K k;
                    try {
                        k = cursor.next();
                    } catch (IllegalStateException e) {
                        // TODO this is a bit ugly
                        if (DataUtils.getErrorCode(e.getMessage()) == DataUtils.ERROR_CHUNK_NOT_FOUND) {
                            parameters.from = currentKey;
                            cursor = map.cursor(parameters);
                            // we (should) get the current key again,
                            // we need to ignore that one
                            if (!cursor.hasNext()) {
                                break;
                            }
                            cursor.next();
                            if (!cursor.hasNext()) {
                                break;
                            }
                            k = cursor.next();
                        } else {
                            throw e;
                        }
                    }
                    final K key = k;
                    TransactionalValue ref = cursor.getValue();
                    TransactionalValue data = getValue(key, ref);
                    if (data != null && data.getValue() != null) {
                        @SuppressWarnings("unchecked")
                        final V value = (V) data.getValue();
                        current = new DataUtils.MapEntry<K, V>(key, value, ref);
                        currentKey = key;
                        return;
                    }
                }
                current = null;
                currentKey = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Entry<K, V> next() {
                Entry<K, V> result = current;
                fetchNext();
                return result;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("Removing is not supported");
            }
        };
    }

    @Override
    public Iterator<K> keyIterator(K from) {
        return keyIterator(from, false);
    }

    @Override
    public Iterator<K> keyIterator(final K from, final boolean includeUncommitted) {
        return new Iterator<K>() {
            private K currentKey = from;
            private StorageMapCursor<K, TransactionalValue> cursor = map.cursor(currentKey);

            {
                fetchNext();
            }

            private void fetchNext() {
                while (cursor.hasNext()) {
                    K k;
                    try {
                        k = cursor.next();
                    } catch (IllegalStateException e) {
                        // TODO this is a bit ugly
                        if (DataUtils.getErrorCode(e.getMessage()) == DataUtils.ERROR_CHUNK_NOT_FOUND) {
                            cursor = map.cursor(currentKey);
                            // we (should) get the current key again,
                            // we need to ignore that one
                            if (!cursor.hasNext()) {
                                break;
                            }
                            cursor.next();
                            if (!cursor.hasNext()) {
                                break;
                            }
                            k = cursor.next();
                        } else {
                            throw e;
                        }
                    }
                    currentKey = k;
                    if (includeUncommitted) {
                        return;
                    }
                    if (containsKey(k)) {
                        return;
                    }
                }
                currentKey = null;
            }

            @Override
            public boolean hasNext() {
                return currentKey != null;
            }

            @Override
            public K next() {
                K result = currentKey;
                fetchNext();
                return result;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("Removing is not supported");
            }
        };
    }
}
