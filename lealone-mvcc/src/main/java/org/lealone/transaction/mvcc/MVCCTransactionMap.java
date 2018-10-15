/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction.mvcc;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.lealone.common.util.DataUtils;
import org.lealone.db.Session;
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

/**
 * A map that supports transactions.
 *
 * @param <K> the key type
 * @param <V> the value type
 * 
 * @author H2 Group
 * @author zhh
 */
public class MVCCTransactionMap<K, V> extends DelegatedStorageMap<K, V> implements TransactionMap<K, V> {

    static class MVCCReplicationMap<K, V> extends MVCCTransactionMap<K, V> {

        private final Session session;
        private final StorageDataType valueType;

        MVCCReplicationMap(MVCCTransaction transaction, StorageMap<K, TransactionalValue> map) {
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

    private final MVCCTransaction transaction;

    /**
     * The map used for writing (the latest version).
     * <p>
     * Key: the key of the data.
     * Value: { transactionId, logId, value }
     */
    protected final StorageMap<K, TransactionalValue> map;

    @SuppressWarnings("unchecked")
    public MVCCTransactionMap(MVCCTransaction transaction, StorageMap<K, TransactionalValue> map) {
        super((StorageMap<K, V>) map);
        this.transaction = transaction;
        this.map = map;
    }

    @Override
    public StorageDataType getValueType() {
        return ((TransactionalValueType) map.getValueType()).valueType;
    }

    /**
     * Get the value for the given key at the time when this map was opened.
     *
     * @param key the key
     * @return the value or null
     */
    @SuppressWarnings("unchecked")
    @Override
    public V get(K key) {
        TransactionalValue data = map.get(key);
        data = getValue(key, data);
        return data == null ? null : (V) data.value;
    }

    /**
     * Get the versioned value for the given key.
     *
     * @param key the key
     * @param data the value stored in the main map
     * @return the value
     */
    protected TransactionalValue getValue(K key, TransactionalValue data) {
        while (true) {
            if (data == null) {
                // doesn't exist or deleted by a committed transaction
                return null;
            }
            long tid = data.tid;
            if (tid == 0) {
                // it is committed
                return data;
            }

            // 数据从节点A迁移到节点B的过程中，如果把A中未提交的值也移到B中，
            // 那么在节点B中会读到不一致的数据，此时需要从节点A读出正确的值
            // TODO 如何更高效的判断，不用比较字符串
            if (data.getHostAndPort() != null && !data.getHostAndPort().equals(NetEndpoint.getLocalTcpHostAndPort())) {
                return getRemoteTransactionalValue(data.getHostAndPort(), key);
            }
            if (tid == transaction.transactionId) {
                return data;
            }

            TransactionalValue v = getValue(key, data, tid);
            if (v != null)
                return v;

            // 底层存储写入了未提交事务的脏数据，并且在事务提交前数据库崩溃了
            if (!transaction.transactionEngine.containsTransaction(tid)) {
                return data.undo(map, key);
            }

            // get the value before the uncommitted transaction
            LinkedList<TransactionalLogRecord> d = transaction.transactionEngine.getTransaction(tid).logRecords;
            if (d == null) {
                // this entry should be committed or rolled back
                // in the meantime (the transaction might still be open)
                // or it might be changed again in a different
                // transaction (possibly one with the same id)
                data = map.get(key);
                if (data != null && data.tid == tid) {
                    // the transaction was not committed correctly
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_CORRUPT,
                            "The transaction log might be corrupt for key {0}", key);
                }
            } else {
                TransactionalLogRecord r = d.get(data.getLogId());
                data = r.oldValue;
            }
        }
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
    public V put(K key, V oldValue, V newValue) {
        TransactionalValue oldTV = TransactionalValue.createCommitted(oldValue);
        boolean ok = trySet(key, newValue, oldTV);
        if (ok) {
            return oldValue;
        }
        throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED, "Entry is locked");
    }

    @SuppressWarnings("unchecked")
    private V set(K key, V value) {
        transaction.checkNotClosed();
        TransactionalValue oldValue = map.get(key);
        boolean ok = trySet(key, value, oldValue);
        if (ok) {
            oldValue = getValue(key, oldValue);
            return oldValue == null ? null : (V) oldValue.value;
        }
        throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED, "Entry is locked");
    }

    /**
     * Try to remove the value for the given key.
     * <p>
     * This will fail if the row is locked by another transaction
     * (that means, if another open transaction changed the row).
     *
     * @param key the key
     * @return whether the entry could be removed
     */
    public boolean tryRemove(K key) {
        return trySet(key, null);
    }

    /**
     * Try to update the value for the given key.
     * <p>
     * This will fail if the row is locked by another transaction
     * (that means, if another open transaction changed the row).
     *
     * @param key the key
     * @param value the new value
     * @return whether the entry could be updated
     */
    public boolean tryPut(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        return trySet(key, value);
    }

    /**
     * Try to set or remove the value. When updating only unchanged entries,
     * then the value is only changed if it was not changed after opening the map.
     *
     * @param key the key
     * @param value the new value (null to remove the value)
     * @return true if the value was set, false if there was a concurrent update
     */
    public boolean trySet(K key, V value) {
        TransactionalValue oldValue = map.get(key);
        return trySet(key, value, oldValue);
    }

    private boolean trySet(K key, V value, TransactionalValue oldValue) {
        TransactionalValue newValue = TransactionalValue.create(transaction, value, oldValue, map.getValueType());
        String mapName = getName();
        if (oldValue == null) {
            // a new value
            transaction.log(mapName, key, oldValue, newValue);
            TransactionalValue old = map.putIfAbsent(key, newValue);
            if (old != null) {
                transaction.logUndo();
                return false;
            }
            return true;
        }
        long tid = oldValue.tid;
        if (tid == 0) {
            // committed
            transaction.log(mapName, key, oldValue, newValue);
            // the transaction is committed:
            // overwrite the value
            if (!map.replace(key, oldValue, newValue)) {
                // somebody else was faster
                transaction.logUndo();
                return false;
            }
            return true;
        }
        if (tid == transaction.transactionId) {
            // added or updated by this transaction
            transaction.log(mapName, key, oldValue, newValue);
            if (!map.replace(key, oldValue, newValue)) {
                // strange, somebody overwrote the value
                // even though the change was not committed
                transaction.logUndo();
                return false;
            }
            return true;
        }

        // the transaction is not yet committed
        return false;
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
        for (MVCCTransaction t : transaction.transactionEngine.getCurrentTransactions()) {
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
                if (data != null && data.value != null) {
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
            for (MVCCTransaction t : transaction.transactionEngine.getCurrentTransactions()) {
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
        TransactionalValue newValue = TransactionalValue.create(transaction, value, null, null);
        K key = map.append(newValue);
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
    public MVCCTransactionMap<K, V> getInstance(Transaction transaction) {
        MVCCTransaction t = (MVCCTransaction) transaction;
        if (t.isShardingMode())
            return new MVCCReplicationMap<>(t, map);
        else
            return new MVCCTransactionMap<>(t, map);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V putCommitted(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        TransactionalValue newValue = TransactionalValue.createCommitted(value);
        TransactionalValue oldValue = map.put(key, newValue);
        return (V) (oldValue == null ? null : oldValue.value);
    }

    @Override
    public boolean isSameTransaction(K key) {
        TransactionalValue data = map.get(key);
        if (data == null) {
            // doesn't exist or deleted by a committed transaction
            return false;
        }

        return data.tid == transaction.transactionId;
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
                    TransactionalValue data = cursor.getValue();
                    data = getValue(key, data);
                    if (data != null && data.value != null) {
                        @SuppressWarnings("unchecked")
                        final V value = (V) data.value;
                        current = new DataUtils.MapEntry<K, V>(key, value);
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
