/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.db.session.Session;
import org.lealone.storage.IterationParameters;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.log.UndoLogRecord;

//只支持单机场景
public class AMTransactionMap<K, V> implements TransactionMap<K, V> {

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
        DataUtils.checkNotNull(value, "value");
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
            addIfAbsent(key, value).onSuccess(r -> listener.operationComplete())
                    .onFailure(t -> listener.operationUndo());
        } else {
            if (tryUpdateOrRemove(key, value, null, oldTransactionalValue, false) == Transaction.OPERATION_COMPLETE)
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
            undoLogSize += t.undoLog.size();
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
                LinkedList<UndoLogRecord> records = t.undoLog.getUndoLogRecords();
                for (UndoLogRecord r : records) {
                    String m = r.getMapName();
                    if (!mapName.equals(m)) {
                        // a different map - ignore
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    K key = (K) r.getKey();
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
            transaction.transactionEngine.removeStorageMap(mapName);
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
    public void setMaxKey(K key) {
        map.setMaxKey(key);
    }

    @Override
    public long getAndAddKey(long delta) {
        return map.getAndAddKey(delta);
    }

    @Override
    public long getDiskSpaceUsed() {
        return map.getDiskSpaceUsed();
    }

    @Override
    public long getMemorySpaceUsed() {
        return map.getMemorySpaceUsed();
    }

    ////////////////////// 以下是分布式API的默认实现 ////////////////////////////////

    @Override
    public Future<Object> get(Session session, Object key) {
        return map.get(session, key);
    }

    @Override
    public Future<Object> put(Session session, Object key, Object value, StorageDataType valueType,
            boolean addIfAbsent) {
        return map.put(session, key, value, valueType, false);
    }

    @Override
    public Future<Object> append(Session session, Object value, StorageDataType valueType) {
        return map.append(session, value, valueType);
    }

    @Override
    public Future<Boolean> replace(Session session, Object key, Object oldValue, Object newValue,
            StorageDataType valueType) {
        return map.replace(session, key, oldValue, newValue, valueType);
    }

    @Override
    public Future<Object> remove(Session session, Object key) {
        return map.remove(session, key);
    }

    @Override
    public void addLeafPage(PageKey pageKey, ByteBuffer page, boolean addPage) {
        map.addLeafPage(pageKey, page, addPage);
    }

    @Override
    public void removeLeafPage(PageKey pageKey) {
        map.removeLeafPage(pageKey);
    }

    @Override
    public LeafPageMovePlan prepareMoveLeafPage(LeafPageMovePlan leafPageMovePlan) {
        return map.prepareMoveLeafPage(leafPageMovePlan);
    }

    @Override
    public ByteBuffer readPage(PageKey pageKey) {
        return map.readPage(pageKey);
    }

    @Override
    public Map<String, List<PageKey>> getNodeToPageKeyMap(Session session, K from, K to) {
        return map.getNodeToPageKeyMap(session, from, to);
    }

    ///////////////////////// 以下是TransactionMap接口API的实现 /////////////////////////

    @Override
    public StorageMap<?, ?> getRawMap() {
        return map;
    }

    @Override
    public long getRawSize() {
        return map.size();
    }

    @Override
    public AMTransactionMap<K, V> getInstance(Transaction transaction) {
        return new AMTransactionMap<>((AMTransaction) transaction, map);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V putCommitted(K key, V value) {
        DataUtils.checkNotNull(value, "value");
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
    public Future<Integer> addIfAbsent(K key, V value) {
        DataUtils.checkNotNull(value, "value");
        transaction.checkNotClosed();
        TransactionalValue ref = TransactionalValue.createRef();
        TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, null, map.getValueType(),
                null, ref);
        ref.setRefValue(newValue);
        String mapName = getName();
        final UndoLogRecord r = transaction.undoLog.add(mapName, key, null, newValue);

        AsyncCallback<Integer> ac = new AsyncCallback<>();
        AsyncHandler<AsyncResult<TransactionalValue>> handler = (ar) -> {
            if (ar.isSucceeded()) {
                TransactionalValue old = ar.getResult();
                if (old != null) {
                    r.setUndone(true);
                    // 同一个事务，先删除再更新，因为删除记录时只是打了一个删除标记，存储层并没有真实删除
                    if (old.getValue() == null) {// || old.getValue() == ValueNull.INSTANCE) { //唯一索引加上这个条件会出错
                                                 // 辅助索引的值是ValueNull.INSTANCE
                        if (tryUpdate(key, value, old) == Transaction.OPERATION_COMPLETE) {
                            ac.setAsyncResult(Transaction.OPERATION_COMPLETE);
                            afterAddComplete();
                        } else {
                            ac.setAsyncResult((Throwable) null);
                        }
                    } else {
                        // 不能用undoLog.undo()，因为它不是线程安全的，
                        // 在undoLog.undo()中执行removeLast()在逻辑上也是不对的，
                        // 因为这里的异步回调函数可能是在不同线程中执行的，顺序也没有保证。
                        ac.setAsyncResult((Throwable) null);
                    }
                } else {
                    ac.setAsyncResult(Transaction.OPERATION_COMPLETE);
                    afterAddComplete();
                }
            } else {
                r.setUndone(true);
                ac.setAsyncResult(ar.getCause());
            }
        };
        map.putIfAbsent(key, ref, handler);
        return ac;
    }

    protected void afterAddComplete() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public K append(V value, AsyncHandler<AsyncResult<K>> handler) {
        TransactionalValue ref = TransactionalValue.createRef(null);
        TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, null, map.getValueType(),
                null, ref);
        ref.setRefValue(newValue);
        K key = map.append(ref, handler);
        transaction.logAppend((StorageMap<Object, TransactionalValue>) map, key, newValue);
        handler.handle(new AsyncResult<>(key));
        return key;
    }

    @Override
    public int tryUpdate(K key, V newValue, int[] columnIndexes, Object oldTransactionalValue, boolean isLockedBySelf) {
        DataUtils.checkNotNull(newValue, "newValue");
        return tryUpdateOrRemove(key, newValue, columnIndexes, (TransactionalValue) oldTransactionalValue,
                isLockedBySelf);
    }

    @Override
    public int tryRemove(K key, Object oldTransactionalValue, boolean isLockedBySelf) {
        return tryUpdateOrRemove(key, null, null, (TransactionalValue) oldTransactionalValue, isLockedBySelf);
    }

    // 在SQL层对应update或delete语句，用于支持行锁和列锁。
    // 如果当前行(或列)已经被其他事务锁住了那么返回一个非Transaction.OPERATION_COMPLETE值表示更新或删除失败了，
    // 当前事务要让出当前线程。
    // 当value为null时代表delete，否则代表update。
    protected int tryUpdateOrRemove(K key, V value, int[] columnIndexes, TransactionalValue oldTransactionalValue,
            boolean isLockedBySelf) {
        DataUtils.checkNotNull(oldTransactionalValue, "oldTransactionalValue");
        transaction.checkNotClosed();
        String mapName = getName();
        // 进入循环前先取出原来的值
        TransactionalValue refValue = oldTransactionalValue.getRefValue();
        if (isLockedBySelf) {
            TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, refValue,
                    map.getValueType(), columnIndexes, oldTransactionalValue);
            transaction.undoLog.add(mapName, key, refValue, newValue);
            if (!oldTransactionalValue.compareAndSet(refValue, newValue))
                throw DbException.throwInternalError();
            return Transaction.OPERATION_COMPLETE;
        }
        // 不同事务更新不同字段时，在循环里重试是可以的
        while (!oldTransactionalValue.isLocked(transaction.transactionId, columnIndexes)) {
            TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, value, refValue,
                    map.getValueType(), columnIndexes, oldTransactionalValue);
            transaction.undoLog.add(mapName, key, refValue, newValue);
            if (oldTransactionalValue.compareAndSet(refValue, newValue)) {
                return Transaction.OPERATION_COMPLETE;
            } else {
                transaction.undoLog.undo();
                if (value == null) {
                    // 两个事务同时删除某一行时，因为删除操作是排它的，
                    // 所以只要一个compareAndSet成功了，另一个就没必要重试了
                    return addWaitingTransaction(key, oldTransactionalValue);
                }
                refValue = oldTransactionalValue.getRefValue();
            }
        }
        // 当前行已经被其他事务锁住了
        return addWaitingTransaction(key, oldTransactionalValue);
    }

    @Override
    public int addWaitingTransaction(Object key, Object oldTransactionalValue, Transaction.Listener listener) {
        return addWaitingTransaction(key, (TransactionalValue) oldTransactionalValue, listener);
    }

    protected int addWaitingTransaction(Object key, TransactionalValue oldTransactionalValue) {
        Object object = Thread.currentThread();
        if (object instanceof Transaction.Listener)
            return addWaitingTransaction(key, oldTransactionalValue, (Transaction.Listener) object);
        else
            return Transaction.OPERATION_NEED_WAIT;
        // throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED, "Entry is locked");
    }

    protected int addWaitingTransaction(Object key, TransactionalValue oldTransactionalValue,
            Transaction.Listener listener) {
        AMTransaction t = transaction.transactionEngine.getTransaction(oldTransactionalValue.getTid());
        // 有可能在这一步事务提交了
        if (t == null)
            return Transaction.OPERATION_NEED_RETRY;
        else
            return t.addWaitingTransaction(key, transaction, listener);
    }

    private int addWaitingTransaction(Object key, TransactionalValue oldTransactionalValue, int[] columnIndexes) {
        Object object = Thread.currentThread();
        if (object instanceof Transaction.Listener) {
            long tid = oldTransactionalValue.getLockOwnerTid(transaction.transactionId, columnIndexes);
            AMTransaction t = transaction.transactionEngine.getTransaction(tid);
            // 有可能在这一步事务提交了
            if (t == null)
                return Transaction.OPERATION_NEED_RETRY;
            else
                return t.addWaitingTransaction(key, transaction, (Transaction.Listener) object);
        } else {
            return Transaction.OPERATION_NEED_WAIT;
            // throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED, "Entry is locked");
        }
    }

    @Override
    public boolean tryLock(K key, Object oldTransactionalValue, boolean addToWaitingQueue, int[] columnIndexes) {
        DataUtils.checkNotNull(oldTransactionalValue, "oldTransactionalValue");
        transaction.checkNotClosed();
        TransactionalValue ref = (TransactionalValue) oldTransactionalValue;
        List<String> retryReplicationNames = ref.getRetryReplicationNames();
        if (retryReplicationNames != null && !retryReplicationNames.isEmpty()) {
            String name = retryReplicationNames.get(0);
            if (name.equals(transaction.getSession().getReplicationName())) {
                TransactionalValue refValue = ref.getRefValue();
                TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, refValue.getValue(),
                        refValue, map.getValueType(), columnIndexes, ref);
                transaction.undoLog.add(getName(), key, refValue, newValue, true);
                if (ref.compareAndSet(refValue, newValue)) {
                    transaction.getSession().setFinalResult(true);
                    retryReplicationNames.remove(0);
                    return true;
                } else {
                    if (addToWaitingQueue
                            && addWaitingTransaction(key, ref, columnIndexes) == Transaction.OPERATION_NEED_RETRY) {
                        return tryLock(key, oldTransactionalValue, addToWaitingQueue, columnIndexes);
                    }
                }
            } else {
                if (addToWaitingQueue
                        && addWaitingTransaction(key, ref, columnIndexes) == Transaction.OPERATION_NEED_RETRY) {
                    return tryLock(key, oldTransactionalValue, addToWaitingQueue, columnIndexes);
                }
                return false;
            }
        }
        if (ref.isLocked(transaction.transactionId, columnIndexes)) {
            if (addToWaitingQueue
                    && addWaitingTransaction(key, ref, columnIndexes) == Transaction.OPERATION_NEED_RETRY) {
                return tryLock(key, oldTransactionalValue, addToWaitingQueue, columnIndexes);
            }
            return false;
        }
        TransactionalValue refValue = ref.getRefValue();
        TransactionalValue newValue = TransactionalValue.createUncommitted(transaction, refValue.getValue(), refValue,
                map.getValueType(), columnIndexes, ref);
        transaction.undoLog.add(getName(), key, refValue, newValue, true);
        if (ref.compareAndSet(refValue, newValue)) {
            return true;
        } else {
            transaction.undoLog.undo();
            if (addToWaitingQueue
                    && addWaitingTransaction(key, ref, columnIndexes) == Transaction.OPERATION_NEED_RETRY) {
                return tryLock(key, oldTransactionalValue, addToWaitingQueue, columnIndexes);
            }
            return false;
        }
    }

    @Override
    public boolean isLocked(Object oldValue, int[] columnIndexes) {
        TransactionalValue tv = ((TransactionalValue) oldValue);
        if (transaction.globalReplicationName != null
                && transaction.globalReplicationName.equals(tv.getGlobalReplicationName()))
            return false;
        return tv.isLocked(transaction.transactionId, columnIndexes);
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

    @Override
    @SuppressWarnings("unchecked")
    public String checkReplicationConflict(ByteBuffer key, String replicationName) {
        String ret;
        K k = (K) getKeyType().read(key);
        TransactionalValue tv = (TransactionalValue) getTransactionalValue(k);
        transaction.setGlobalReplicationName(replicationName);
        if (tryLock(k, tv)) {
            ret = replicationName;
        } else {
            ret = tv.getGlobalReplicationName();
        }
        return ret;
    }
}
