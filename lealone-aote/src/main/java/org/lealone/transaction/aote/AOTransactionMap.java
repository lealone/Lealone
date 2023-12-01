/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import org.lealone.common.util.DataUtils;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.db.scheduler.SchedulerListener;
import org.lealone.db.session.Session;
import org.lealone.storage.CursorParameters;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.page.IPage;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.TransactionMapCursor;
import org.lealone.transaction.aote.log.UndoLog;
import org.lealone.transaction.aote.log.UndoLogRecord;

public class AOTransactionMap<K, V> implements TransactionMap<K, V> {

    private final AOTransaction transaction;
    private final StorageMap<K, TransactionalValue> map;

    public AOTransactionMap(AOTransaction transaction, StorageMap<K, TransactionalValue> map) {
        this.transaction = transaction;
        this.map = map;
    }

    public AOTransaction getTransaction() {
        return transaction;
    }

    ///////////////////////// 以下是StorageMap接口API的实现 ，有一部分是直接委派的，在后面列出 /////////////////////////

    @Override
    public StorageDataType getValueType() {
        return getTransactionalValueType().valueType;
    }

    @Override
    public TransactionalValueType getTransactionalValueType() {
        return (TransactionalValueType) map.getValueType();
    }

    @Override
    public V get(K key) {
        TransactionalValue tv = map.get(key);
        return getUnwrapValue(key, tv);
    }

    // 外部传进来的值被包装成TransactionalValue了，所以需要拆出来
    @SuppressWarnings("unchecked")
    private V getUnwrapValue(K key, TransactionalValue tv) {
        Object v = getValue(key, tv);
        return (V) v;
    }

    // 获得当前事务能看到的值，依据不同的隔离级别看到的值是不一样的
    protected Object getValue(K key, TransactionalValue tv) {
        // tv为null说明记录不存在
        if (tv == null)
            return null;

        // 如果tv是未提交的，并且就是当前事务，那么这里也会返回未提交的值
        Object v = tv.getValue(transaction, map);
        if (v != null) {
            // 前面的事务已经提交了，但是因为当前事务隔离级别的原因它看不到
            if (v == TransactionalValue.SIGHTLESS)
                return null;
            else
                return v;
        }

        // 已经删除
        if (tv.isCommitted() && tv.getValue() == null)
            return null;

        // 运行到这里时，当前事务看不到任何值，可能是事务隔离级别太高了
        return null;
    }

    @Override
    public K firstKey() {
        TransactionMapCursor<K, V> cursor = cursor();
        return cursor.next() ? cursor.getKey() : null;
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
        while (true) {
            K k = map.higherKey(key);
            if (k == null || get(k) != null) {
                return k;
            }
            key = k;
        }
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
    // 最初的实现方案是遍历UndoLog的记录来确定size，但是UndoLog是为单线程设计的，所以存在并发bug，
    // 新的方案当存在多个事务时虽然慢了一些，但是实现不用搞得很复杂，能减少错误
    @Override
    public long size() {
        long undoLogSize = 0;
        for (AOTransaction t : transaction.transactionEngine.currentTransactions()) {
            UndoLog ul = t.undoLog;
            if (ul != null)
                undoLogSize += ul.size();
        }
        if (undoLogSize == 0)
            return map.size(); // 存在的多个事务都是只读操作时可以安全返回原表的size

        long size = 0;
        TransactionMapCursor<?, ?> cursor = cursor();
        while (cursor.next()) {
            size++;
        }
        return size;
    }

    @Override
    public void decrementSize() {
        map.decrementSize();
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
    public TransactionMapCursor<K, V> cursor(CursorParameters<K> parameters) {
        return new TransactionMapCursor<K, V>() {
            final StorageMapCursor<K, TransactionalValue> cursor = map.cursor(parameters);
            V value;

            @Override
            public K getKey() {
                return cursor.getKey();
            }

            @Override
            public V getValue() {
                return value;
            }

            @Override
            public TransactionalValue getTValue() {
                return cursor.getValue();
            }

            @Override
            public IPage getPage() {
                return cursor.getPage();
            }

            @Override
            @SuppressWarnings("unchecked")
            public boolean next() {
                while (cursor.next()) {
                    // 过滤掉已标记为删除的记录
                    value = (V) AOTransactionMap.this.getValue(cursor.getKey(), cursor.getValue());
                    if (value != null)
                        return true;
                }
                return false;
            }
        };
    }

    @Override
    public void remove() {
        // 提前获取map名，一些存储引擎调用完 map.remove()后，再调用map.getName()会返回null
        String mapName = map.getName();
        if (mapName != null) {
            map.remove();
            transaction.transactionEngine.removeStorageMap(transaction, mapName);
        }
    }

    ///////////////////////// 以下是直接委派的StorageMap接口API /////////////////////////

    @Override
    public void clear() {
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
    public void repair() {
        map.repair();
    }

    @Override
    public void gc(TransactionEngine te) {
        map.gc(te);
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

    @Override
    public boolean hasUnsavedChanges() {
        return map.hasUnsavedChanges();
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
    public AOTransactionMap<K, V> getInstance(Transaction transaction) {
        return new AOTransactionMap<>((AOTransaction) transaction, map);
    }

    @Override // 比put方法更高效，不需要返回值，所以也不需要事先调用get
    public Future<Integer> addIfAbsent(K key, V value) {
        DataUtils.checkNotNull(value, "value");
        transaction.checkNotClosed();
        TransactionalValue newTV;
        UndoLogRecord r;
        Session session = transaction.getSession();
        if (session == null || session.isUndoLogEnabled()) {
            newTV = new TransactionalValue(value, transaction); // 内部有增加行锁
            r = transaction.undoLog.add(map, key, null, newTV);
        } else {
            newTV = new TransactionalValue(value); // 内部没有增加行锁
            r = null;
        }
        AsyncCallback<Integer> ac = transaction.createCallback();
        AsyncHandler<AsyncResult<TransactionalValue>> handler = ar -> {
            if (ar.isSucceeded()) {
                TransactionalValue old = ar.getResult();
                if (old != null) {
                    // 在提交或回滚时直接忽略即可
                    if (r != null)
                        r.setUndone(true);
                    // 同一个事务，先删除再更新，因为删除记录时只是打了一个删除标记，存储层并没有真实删除
                    if (old.getValue() == null) {
                        old.setValue(value);
                        if (r != null)
                            transaction.undoLog.add(map, key, old.getOldValue(), old);
                        ac.setAsyncResult(Transaction.OPERATION_COMPLETE);
                    } else {
                        ac.setAsyncResult((Throwable) null);
                    }
                } else {
                    ac.setAsyncResult(Transaction.OPERATION_COMPLETE);
                }
            } else {
                if (r != null)
                    r.setUndone(true);
                ac.setAsyncResult(ar.getCause());
            }
        };
        map.putIfAbsent(transaction.getSession(), key, newTV, handler);
        return ac;
    }

    @Override
    public int tryUpdate(K key, V newValue, int[] columnIndexes, Object oldTValue,
            boolean isLockedBySelf) {
        DataUtils.checkNotNull(newValue, "newValue");
        return tryUpdateOrRemove(key, newValue, columnIndexes, oldTValue, isLockedBySelf);
    }

    @Override
    public int tryRemove(K key, Object oldTValue, boolean isLockedBySelf) {
        return tryUpdateOrRemove(key, null, null, oldTValue, isLockedBySelf);
    }

    // 在SQL层对应update或delete语句，用于支持行锁和列锁。
    // 如果当前行(或列)已经被其他事务锁住了那么返回一个非Transaction.OPERATION_COMPLETE值表示更新或删除失败了，
    // 当前事务要让出当前线程。
    // 当value为null时代表delete，否则代表update。
    protected int tryUpdateOrRemove(K key, V value, int[] columnIndexes, Object oldTValue,
            boolean isLockedBySelf) {
        DataUtils.checkNotNull(oldTValue, "oldTValue");
        transaction.checkNotClosed();
        TransactionalValue tv = (TransactionalValue) oldTValue;
        // 提前调用tryLock的场景直接跳过
        if (!isLockedBySelf && tv.tryLock(transaction) != 1) {
            // 当前行已经被其他事务锁住了
            return Transaction.OPERATION_NEED_WAIT;
        }
        Object oldValue = tv.getValue();
        tv.setTransaction(transaction); // 二级索引需要设置
        tv.setValue(value);
        transaction.undoLog.add(map, key, oldValue, tv);
        return Transaction.OPERATION_COMPLETE;
    }

    @Override
    public int addWaitingTransaction(Object key, Object oldTValue) {
        return ((TransactionalValue) oldTValue).addWaitingTransaction(key, transaction);
    }

    @Override
    public int tryLock(K key, Object oldTValue, int[] columnIndexes) {
        DataUtils.checkNotNull(oldTValue, "oldTValue");
        transaction.checkNotClosed();
        TransactionalValue tv = (TransactionalValue) oldTValue;
        return tv.tryLock(transaction);
    }

    @Override
    public boolean isLocked(Object oldTValue, int[] columnIndexes) {
        TransactionalValue tv = (TransactionalValue) oldTValue;
        return tv.isLocked(transaction);
    }

    @Override
    public Object[] getObjects(K key, int[] columnIndexes) {
        Object[] objects = map.getObjects(key, columnIndexes);
        TransactionalValue tv = (TransactionalValue) objects[1];
        return new Object[] { objects[0], tv, getUnwrapValue(key, tv) };
    }

    @Override
    public Object getTransactionalValue(K key) {
        return map.get(key);
    }

    //////////////////// 以下是StorageMap与写操作相关的同步和异步API的实现 ////////////////////////////////

    @Override
    public V put(K key, V value) {
        return put0(transaction.getSession(), key, value, null);
    }

    @Override
    public void put(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        put0(transaction.getSession(), key, value, handler);
    }

    @Override
    public void put(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        put0(session, key, value, handler);
    }

    private V put0(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        DataUtils.checkNotNull(value, "value");
        return writeOperation(session, key, value, handler);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent0(transaction.getSession(), key, value, null);
    }

    @Override
    public void putIfAbsent(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        putIfAbsent0(transaction.getSession(), key, value, handler);
    }

    @Override
    public void putIfAbsent(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        putIfAbsent0(session, key, value, handler);
    }

    private V putIfAbsent0(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        V v = get(key);
        if (v == null) {
            DataUtils.checkNotNull(value, "value");
            v = writeOperation(session, key, value, handler);
        }
        return v;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return replace0(transaction.getSession(), key, oldValue, newValue, null);
    }

    @Override
    public void replace(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        replace0(transaction.getSession(), key, oldValue, newValue, handler);
    }

    @Override
    public void replace(Session session, K key, V oldValue, V newValue,
            AsyncHandler<AsyncResult<Boolean>> handler) {
        replace0(session, key, oldValue, newValue, handler);
    }

    private boolean replace0(Session session, K key, V oldValue, V newValue,
            AsyncHandler<AsyncResult<Boolean>> handler) {
        V old = get(key);
        if (areValuesEqual(old, oldValue)) {
            DataUtils.checkNotNull(newValue, "value");
            writeOperation(session, key, newValue, ar -> {
                if (ar.isSucceeded())
                    handler.handle(new AsyncResult<>(true));
                else
                    handler.handle(new AsyncResult<>(ar.getCause()));
            });
            return true;
        }
        return false;
    }

    @Override
    public K append(V value) {
        return append0(transaction.getSession(), value, null);
    }

    @Override
    public void append(V value, AsyncHandler<AsyncResult<K>> handler) {
        append0(transaction.getSession(), value, handler);
    }

    @Override
    public void append(Session session, V value, AsyncHandler<AsyncResult<K>> handler) {
        append0(session, value, handler);
    }

    // 追加新记录时不会产生事务冲突
    private K append0(Session session, V value, AsyncHandler<AsyncResult<K>> handler) {
        DataUtils.checkNotNull(value, "value");
        boolean isUndoLogEnabled = (session == null || session.isUndoLogEnabled());
        TransactionalValue newTV;
        if (isUndoLogEnabled)
            newTV = new TransactionalValue(value, transaction); // 内部有增加行锁
        else
            newTV = new TransactionalValue(value); // 内部没有增加行锁
        if (handler != null) {
            map.append(session, newTV, ar -> {
                if (isUndoLogEnabled && ar.isSucceeded())
                    transaction.undoLog.add(map, ar.getResult(), null, newTV);
                handler.handle(ar);
            });
            return null;
        } else {
            K key = map.append(newTV);
            // 记事务log和append新值都是更新内存中的相应数据结构，所以不必把log调用放在append前面
            // 放在前面的话调用log方法时就不知道key是什么，当事务要rollback时就不知道如何修改map的内存数据
            if (isUndoLogEnabled)
                transaction.undoLog.add(map, key, null, newTV);
            return key;
        }
    }

    @Override
    public V remove(K key) {
        return remove0(transaction.getSession(), key, null);
    }

    @Override
    public void remove(K key, AsyncHandler<AsyncResult<V>> handler) {
        remove0(transaction.getSession(), key, handler);
    }

    @Override
    public void remove(Session session, K key, AsyncHandler<AsyncResult<V>> handler) {
        remove0(session, key, handler);
    }

    private V remove0(Session session, K key, AsyncHandler<AsyncResult<V>> handler) {
        return writeOperation(session, key, null, handler);
    }

    private V writeOperation(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        TransactionalValue oldTValue = map.get(key);
        // tryUpdateOrRemove可能会改变oldValue的内部状态，所以提前拿到返回值
        V retValue = getUnwrapValue(key, oldTValue);

        if (handler != null) {
            writeOperation(session, key, value, handler, oldTValue, retValue);
            return retValue;
        } else {
            SchedulerListener<V> listener = SchedulerListener.createSchedulerListener();
            writeOperation(session, key, value, listener, oldTValue, retValue);
            return listener.await();
        }
    }

    private void writeOperation(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler,
            TransactionalValue oldTValue, V retValue) {
        // insert
        if (oldTValue == null) {
            addIfAbsent(key, value).onSuccess(r -> handler.handle(new AsyncResult<>(retValue)))
                    .onFailure(t -> handler.handle(new AsyncResult<>(t)));
        } else {
            if (tryUpdateOrRemove(key, value, null, oldTValue, false) == Transaction.OPERATION_COMPLETE)
                handler.handle(new AsyncResult<>(retValue));
            else
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED,
                        "Entry is locked");
        }
    }
}
