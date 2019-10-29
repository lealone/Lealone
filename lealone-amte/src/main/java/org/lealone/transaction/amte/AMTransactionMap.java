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
import java.util.concurrent.atomic.AtomicBoolean;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.Session;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.storage.DistributedStorageMap;
import org.lealone.storage.IterationParameters;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;

public class AMTransactionMap<K, V> implements TransactionMap<K, V> {

    static class AMReplicationMap<K, V> extends AMTransactionMap<K, V> {

        private final DistributedStorageMap<K, TransactionalValue> map;
        private final Session session;
        private final StorageDataType valueType;

        AMReplicationMap(AMTransaction transaction, StorageMap<K, TransactionalValue> map) {
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
    }

    private final AMTransaction transaction;
    protected final StorageMap<K, TransactionalValue> map;

    public AMTransactionMap(AMTransaction transaction, StorageMap<K, TransactionalValue> map) {
        this.transaction = transaction;
        this.map = map;
    }

    ///////////////////////// 以下是StorageMap接口API的实现 ，有一部分是直接委派的，在后面列出 /////////////////////////

    @Override
    public StorageDataType getValueType() {
        return ((TransactionalValueType) map.getValueType()).valueType;
    }

    @Override
    public V get(K key) {
        TransactionalValue ref = map.get(key);
        return getUnwrapValue(key, ref);
    }

    @Override
    public V get(K key, int[] columnIndexes) {
        TransactionalValue ref = map.get(key, columnIndexes);
        return getUnwrapValue(key, ref);
    }

    // 外部传进来的值被包装成TransactionalValue了，所以需要拆出来
    @SuppressWarnings("unchecked")
    private V getUnwrapValue(K key, TransactionalValue data) {
        TransactionalValue tv = getValue(key, data);
        return tv == null ? null : (V) tv.getValue();
    }

    // 获得当前事务能看到的值，依据不同的隔离级别看到的值是不一样的
    protected TransactionalValue getValue(K key, TransactionalValue data) {
        // data为null说明记录不存在，data.getRefValue()为null说明是一个删除标记
        if (data == null || data.getRefValue() == null) {
            return null;
        }

        // 如果data是未提交的，并且就是当前事务，那么这里也会返回未提交的值
        TransactionalValue tv = data.getCommitted(transaction);
        if (tv != null) {
            // 前面的事务已经提交了，但是因为当前事务隔离级别的原因它看不到
            if (tv == TransactionalValue.SIGHTLESS)
                return null;
            else
                return tv;
        }

        // 复制和分布式事务的场景
        tv = getDistributedValue(key, data);
        if (tv != null)
            return tv;

        // 有些底层存储引擎可能会在事务提交前就把脏数据存盘了，
        // 然后还没等事务提交，数据库又崩溃了，那么在这里需要撤销才能得到正确的值，
        // 这一过程被称为: 读时撤销
        if (!transaction.transactionEngine.containsTransaction(data.getTid())) {
            return data.undo(map, key);
        }
        // 运行到这里时，当前事务看不到任何值，可能是事务隔离级别太高了
        return null;
    }

    // AMTE是单机版的事务引擎，不支持这个功能
    protected TransactionalValue getDistributedValue(K key, TransactionalValue data) {
        return null;
    }

    @Override
    public V put(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        return setSync(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V v = get(key);
        if (v == null)
            v = put(key, value);
        return v;
    }

    @Override
    public V remove(K key) {
        return setSync(key, null);
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

    private V setSync(K key, V value) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean result = new AtomicBoolean();
        Transaction.Listener listener = new Transaction.Listener() {
            @Override
            public void operationUndo() {
                result.set(false);
                latch.countDown();
            }

            @Override
            public void operationComplete() {
                result.set(true);
                latch.countDown();
            }
        };

        TransactionalValue oldTransactionalValue = map.get(key);
        // tryUpdateOrRemove可能会改变oldValue的内部状态，所以提前拿到返回值
        V retValue = getUnwrapValue(key, oldTransactionalValue);
        // insert
        if (oldTransactionalValue == null) {
            addIfAbsent(key, value, listener);
        } else {
            if (tryUpdateOrRemove(key, value, null, oldTransactionalValue) == Transaction.OPERATION_COMPLETE)
                listener.operationComplete();
            else
                listener.operationUndo();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            DbException.convert(e);
        }
        if (result.get()) {
            return retValue;
        }
        throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED, "Entry is locked");
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

    /**
     * Get the size of the map as seen by this transaction.
     *
     * @return the size
     */
    @Override
    public long size() {
        long sizeRaw = map.size();
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
        long size = map.size();
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
        return size() == 0;
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

    ///////////////////////// 以下是直接委派的StorageMap接口API /////////////////////////

    @Override
    public void clear() {
        // TODO 可以rollback吗?
        map.clear();
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
    public Storage getStorage() {
        return map.getStorage();
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        return map.areValuesEqual(a, b);
    }

    @Override
    public boolean isInMemory() {
        return map.isInMemory();
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
    public void setMaxKey(Object key) {
        map.setMaxKey(key);
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
    public StorageMap<Object, Object> getRawMap() {
        return map.getRawMap();
    }

    ///////////////////////// 以下是TransactionMap接口API的实现 /////////////////////////

    @Override
    public long rawSize() {
        return map.size();
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

    @Override // 比put方法更高效，不需要返回值，所以也不需要事先调用get
    public void addIfAbsent(K key, V value, Transaction.Listener listener) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        transaction.checkNotClosed();
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

    @Override
    @SuppressWarnings("unchecked")
    public K append(V value, Transaction.Listener listener) {
        TransactionalValue ref = TransactionalValue.createRef(null);
        TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, null, map.getValueType(),
                null, ref);
        ref.setRefValue(newValue);
        AsyncHandler<AsyncResult<TransactionalValue>> handler = (ar) -> {
            if (ar.isSucceeded()) {
                listener.operationComplete();
            } else {
                listener.operationUndo();
            }
        };
        K key = map.append(ref, handler);
        transaction.logAppend((StorageMap<Object, TransactionalValue>) map, key, newValue);
        return key;
    }

    @Override
    public int tryUpdate(K key, V newValue, int[] columnIndexes, Object oldTransactionalValue) {
        DataUtils.checkArgument(newValue != null, "The newValue may not be null");
        return tryUpdateOrRemove(key, newValue, columnIndexes, (TransactionalValue) oldTransactionalValue);
    }

    @Override
    public int tryRemove(K key, Object oldTransactionalValue) {
        return tryUpdateOrRemove(key, null, null, (TransactionalValue) oldTransactionalValue);
    }

    // 在SQL层对应update或delete语句，用于支持行锁和列锁。
    // 如果当前行(或列)已经被其他事务锁住了那么返回false表示更新或删除失败了，当前事务要让出当前线程。
    // 当value为null时代表delete，否则代表update
    private int tryUpdateOrRemove(K key, V value, int[] columnIndexes, TransactionalValue oldTransactionalValue) {
        DataUtils.checkArgument(oldTransactionalValue != null, "The oldTransactionalValue may not be null");
        transaction.checkNotClosed();
        String mapName = getName();
        // 不同事务更新不同字段时，在循环里重试是可以的
        while (!oldTransactionalValue.isLocked(transaction.transactionId, columnIndexes)) {
            TransactionalValue refValue = oldTransactionalValue.getRefValue();
            TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, refValue,
                    map.getValueType(), columnIndexes, oldTransactionalValue);
            transaction.log(mapName, key, refValue, newValue);
            if (oldTransactionalValue.compareAndSet(refValue, newValue)) {
                return Transaction.OPERATION_COMPLETE;
            } else {
                transaction.logUndo();
                if (value == null) {
                    // 两个事务同时删除某一行时，因为删除操作是排它的，
                    // 所以只要一个compareAndSet成功了，另一个就没必要重试了
                    return addWaitingTransaction(key, oldTransactionalValue);
                }
            }
        }
        // 当前行已经被其他事务锁住了
        return addWaitingTransaction(key, oldTransactionalValue);
    }

    @Override
    public int addWaitingTransaction(Object key, Object oldTransactionalValue, Transaction.Listener listener) {
        return addWaitingTransaction(key, (TransactionalValue) oldTransactionalValue, listener);
    }

    private int addWaitingTransaction(Object key, TransactionalValue oldTransactionalValue) {
        Object object = Thread.currentThread();
        if (object instanceof Transaction.Listener)
            return addWaitingTransaction(key, oldTransactionalValue, (Transaction.Listener) object);
        else
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED, "Entry is locked");

    }

    private int addWaitingTransaction(Object key, TransactionalValue oldTransactionalValue,
            Transaction.Listener listener) {
        AMTransaction t = transaction.transactionEngine.getTransaction(oldTransactionalValue.getTid());
        return t.addWaitingTransaction(key, transaction, listener);
    }

    @Override
    public boolean tryLock(K key, Object oldTransactionalValue) {
        DataUtils.checkArgument(oldTransactionalValue != null, "The oldTransactionalValue may not be null");
        transaction.checkNotClosed();
        TransactionalValue ref = (TransactionalValue) oldTransactionalValue;
        if (ref.isLocked(transaction.transactionId, null))
            return false;

        TransactionalValue refValue = ref.getRefValue();
        TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, refValue.getValue(), refValue,
                map.getValueType(), null, ref);
        transaction.log(getName(), key, refValue, newValue, true);
        if (ref.compareAndSet(refValue, newValue)) {
            return true;
        } else {
            transaction.logUndo();
            return false;
        }
    }

    @Override
    public boolean isLocked(Object oldValue, int[] columnIndexes) {
        return ((TransactionalValue) oldValue).isLocked(transaction.transactionId, columnIndexes);
    }

    @Override
    public Object[] getValueAndRef(K key, int[] columnIndexes) {
        TransactionalValue ref = map.get(key, columnIndexes);
        return new Object[] { getUnwrapValue(key, ref), ref };
    }

    @Override
    public Object getValue(Object oldTransactionalValue) {
        return ((TransactionalValue) oldTransactionalValue).getValue();
    }

    @Override
    public Object getTransactionalValue(K key) {
        return map.get(key);
    }
}
