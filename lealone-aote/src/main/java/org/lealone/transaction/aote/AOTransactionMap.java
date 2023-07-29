/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.db.session.Session;
import org.lealone.storage.CursorParameters;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.page.DirtyPageHandler;
import org.lealone.storage.page.IPage;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionListener;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.TransactionMapCursor;
import org.lealone.transaction.aote.log.UndoLogRecord;

public class AOTransactionMap<K, V> implements TransactionMap<K, V> {

    private final AOTransaction transaction;
    private final StorageMap<K, TransactionalValue> map;

    public AOTransactionMap(AOTransaction transaction, StorageMap<K, TransactionalValue> map) {
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
        Object v = tv.getValue(transaction);
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
        TransactionListener listener = new TransactionListener() {
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

        TransactionalValue oldTValue = map.get(key);
        // tryUpdateOrRemove可能会改变oldValue的内部状态，所以提前拿到返回值
        V retValue = getUnwrapValue(key, oldTValue);
        // insert
        if (oldTValue == null) {
            addIfAbsent(key, value).onSuccess(r -> listener.operationComplete())
                    .onFailure(t -> listener.operationUndo());
        } else {
            if (tryUpdateOrRemove(key, value, null, oldTValue, false) == Transaction.OPERATION_COMPLETE)
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
        for (AOTransaction t : transaction.transactionEngine.getCurrentTransactions()) {
            undoLogSize += t.undoLog.size();
        }
        if (undoLogSize == 0) {
            return sizeRaw;
        }
        if (undoLogSize > sizeRaw) {
            // the undo log is larger than the map -
            // count the entries of the map
            long size = 0;
            TransactionMapCursor<?, ?> cursor = cursor();
            while (cursor.next()) {
                size++;
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
        StorageMap<Object, Integer> temp = storage.openMap(tmpMapName, new ObjectDataType(),
                new ObjectDataType(), null);
        try {
            for (AOTransaction t : transaction.transactionEngine.getCurrentTransactions()) {
                UndoLogRecord r = t.undoLog.getFirst();
                while (r != null) {
                    String m = r.getMapName();
                    if (!mapName.equals(m)) {
                        r = r.getNext();
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
                    r = r.getNext();
                }
            }
        } finally {
            temp.remove();
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
            transaction.transactionEngine.removeStorageMap(mapName);
        }
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
    public void repair() {
        map.repair();
    }

    @Override
    public void gc(ConcurrentSkipListMap<Long, ? extends Transaction> currentTransactions) {
        map.gc(currentTransactions);
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
    public long getDirtyMemorySpaceUsed() {
        return map.getDirtyMemorySpaceUsed();
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
        final TransactionalValue newTV;
        final UndoLogRecord r;
        final Session session = transaction.getSession();
        if (session == null || session.isUndoLogEnabled()) {
            newTV = new TransactionalValue(value, transaction);
            r = transaction.undoLog.add(getName(), key, null, newTV);
        } else {
            newTV = new TransactionalValue(value);
            r = null;
        }

        AsyncCallback<Integer> ac = new AsyncCallback<>();
        DirtyPageHandler<AsyncResult<TransactionalValue>> handler = //
                new DirtyPageHandler<AsyncResult<TransactionalValue>>() {
                    @Override
                    public void handle(AsyncResult<TransactionalValue> ar) {
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
                                        transaction.undoLog.add(getName(), key, old.getOldValue(), old);
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
                    }

                    @Override
                    public void addDirtyPage(IPage page) {
                        if (session != null) {
                            session.addDirtyPage(page);
                        }
                    }
                };
        map.putIfAbsent(key, newTV, handler);
        return ac;
    }

    @Override
    public K append(V value) {
        return append0(value, null);
    }

    @Override
    public void append(V value, AsyncHandler<AsyncResult<K>> handler) {
        append0(value, handler);
    }

    private K append0(V value, AsyncHandler<AsyncResult<K>> handler) { // 追加新记录时不会产生事务冲突
        final Session session = transaction.getSession();
        boolean isUndoLogEnabled = (session == null || session.isUndoLogEnabled());
        TransactionalValue newTV = new TransactionalValue(value, transaction);
        if (handler != null) {
            DirtyPageHandler<AsyncResult<K>> dirtyPageHandler = //
                    new DirtyPageHandler<AsyncResult<K>>() {
                        @Override
                        public void handle(AsyncResult<K> ar) {
                            if (ar.isSucceeded() && isUndoLogEnabled)
                                transaction.undoLog.add(getName(), ar.getResult(), null, newTV);
                            handler.handle(ar);
                        }

                        @Override
                        public void addDirtyPage(IPage page) {
                            if (session != null) {
                                session.addDirtyPage(page);
                            }
                        }
                    };
            map.append(newTV, dirtyPageHandler);
            return null;
        } else {
            K key = map.append(newTV);
            // 记事务log和append新值都是更新内存中的相应数据结构，所以不必把log调用放在append前面
            // 放在前面的话调用log方法时就不知道key是什么，当事务要rollback时就不知道如何修改map的内存数据
            if (isUndoLogEnabled)
                transaction.undoLog.add(getName(), key, null, newTV);
            return key;
        }
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
            return addWaitingTransaction(key, tv);
        }
        Object oldValue = tv.getValue();
        tv.setTransaction(transaction); // 二级索引需要设置
        tv.setValue(value);
        transaction.undoLog.add(getName(), key, oldValue, tv);
        return Transaction.OPERATION_COMPLETE;
    }

    @Override
    public int addWaitingTransaction(Object key, Object oldTValue) {
        return addWaitingTransaction(key, (TransactionalValue) oldTValue);
    }

    private int addWaitingTransaction(Object key, TransactionalValue oldTValue) {
        Object object = Thread.currentThread();
        if (!(object instanceof TransactionListener)) {
            return Transaction.OPERATION_NEED_WAIT;
        }
        AOTransaction t = oldTValue.getLockOwner();
        // 有可能在这一步事务提交了
        if (t == null)
            return Transaction.OPERATION_NEED_RETRY;
        else
            return t.addWaitingTransaction(key, transaction, (TransactionListener) object);
    }

    @Override
    public int tryLock(K key, Object oldTValue, int[] columnIndexes) {
        DataUtils.checkNotNull(oldTValue, "oldTValue");
        transaction.checkNotClosed();
        TransactionalValue tv = (TransactionalValue) oldTValue;

        int ret = tv.tryLock(transaction);
        if (ret == 0) {
            // 就算调用此方法的过程中解锁了也不能直接调用tryLock重试，需要返回到上层，然后由上层决定如何重试
            // 因为更新或删除或select for update可能是带有条件的，根据修改后的新记录判断才能决定是否重试
            addWaitingTransaction(key, tv);
        }
        return ret;
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
}
