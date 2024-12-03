/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.lealone.common.util.DataUtils;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.lock.Lockable;
import com.lealone.db.scheduler.SchedulerListener;
import com.lealone.db.session.InternalSession;
import com.lealone.storage.CursorParameters;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageMapCursor;
import com.lealone.storage.page.PageListener;
import com.lealone.storage.type.StorageDataType;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.TransactionMap;
import com.lealone.transaction.TransactionMapCursor;
import com.lealone.transaction.aote.TransactionalValue.OldValue;
import com.lealone.transaction.aote.log.UndoLog;
import com.lealone.transaction.aote.log.UndoLogRecord;

public class AOTransactionMap<K, V> implements TransactionMap<K, V> {

    private final AOTransaction transaction;
    private final StorageMap<K, Lockable> map;

    public AOTransactionMap(AOTransaction transaction, StorageMap<K, Lockable> map) {
        this.transaction = transaction;
        this.map = map;
    }

    public AOTransaction getTransaction() {
        return transaction;
    }

    ///////////////////////// 以下是StorageMap接口API的实现 ，有一部分是直接委派的，在后面列出 /////////////////////////

    @Override
    public StorageDataType getValueType() {
        return map.getValueType().getRawType();
    }

    @Override
    public V get(K key) {
        Lockable tv = map.get(key);
        return getUnwrapValue(key, tv);
    }

    // 外部传进来的值被包装成TransactionalValue了，所以需要拆出来
    @SuppressWarnings("unchecked")
    private V getUnwrapValue(K key, Lockable tv) {
        Object v = getValue(key, tv);
        return (V) v;
    }

    // 获得当前事务能看到的值，依据不同的隔离级别看到的值是不一样的
    protected Object getValue(K key, Lockable lockable) {
        // 为null说明记录不存在
        if (lockable == null)
            return null;

        // 如果lockable是未提交的，并且就是当前事务，那么这里也会返回未提交的值
        Object v = TransactionalValue.getValue(lockable, transaction, map);
        if (v != null) {
            // 前面的事务已经提交了，但是因为当前事务隔离级别的原因它看不到
            if (v == TransactionalValue.SIGHTLESS)
                return null;
            else
                return v;
        }
        // 运行到这里时，当前事务看不到任何值，可能是事务隔离级别太高了或者已经删除
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
            final StorageMapCursor<K, Lockable> cursor = map.cursor(parameters);
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
    public void gc() {
        map.gc();
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

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key, int[] columnIndexes) {
        Lockable lockable = map.get(key, columnIndexes);
        return (V) getValue(key, lockable);
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

    @Override // put和putIfAbsent需要先执行get，而addIfAbsent不需要
    public void addIfAbsent(K key, Lockable lockable, AsyncResultHandler<Integer> handler) {
        add(key, lockable, true, null, handler);
    }

    @SuppressWarnings("unchecked")
    private void add(K key, Lockable lockable, boolean ifAbsent, AtomicReference<V> vRef,
            AsyncResultHandler<Integer> topHandler) {
        DataUtils.checkNotNull(lockable, "lockable");
        transaction.checkNotClosed();
        UndoLogRecord r;
        InternalSession session = transaction.getSession();
        if (session == null || session.isUndoLogEnabled()) {
            r = addUndoLog(key, lockable, null);
            TransactionalValue.insertLock(lockable, transaction); // 内部有增加行锁
        } else {
            r = null;
        }
        AsyncResultHandler<Lockable> handler = ar -> {
            if (ar.isSucceeded()) {
                Lockable old = ar.getResult();
                if (old != null) {
                    if (vRef != null)
                        vRef.set((V) old.getLockedValue());
                    if (ifAbsent) {
                        // 在提交或回滚时直接忽略即可
                        if (r != null)
                            r.setUndone(true);
                        // 同一个事务，先删除再更新，因为删除记录时只是打了一个删除标记，存储层并没有真实删除
                        if (old.getLockedValue() == null) {
                            old.setLockedValue(lockable.getLockedValue());
                            if (r != null) {
                                addUndoLog(key, old, lockable.getLockedValue());
                            }
                        } else {
                            topHandler.handleResult(Transaction.OPERATION_DATA_DUPLICATE);
                            return;
                        }
                    }
                }
                topHandler.handleResult(Transaction.OPERATION_COMPLETE);
            } else {
                if (r != null)
                    r.setUndone(true);
                topHandler.handleException(ar.getCause());
            }
        };
        if (ifAbsent)
            map.putIfAbsent(session, key, lockable, handler);
        else
            map.put(session, key, lockable, handler);
    }

    @Override
    public int tryUpdate(K key, V newValue, Lockable lockable, boolean isLockedBySelf) {
        DataUtils.checkNotNull(newValue, "newValue");
        return tryUpdateOrRemove(key, newValue, lockable, isLockedBySelf);
    }

    @Override
    public int tryRemove(K key, Lockable lockable, boolean isLockedBySelf) {
        return tryUpdateOrRemove(key, null, lockable, isLockedBySelf);
    }

    // 在SQL层对应update或delete语句，当value为null时代表delete，否则代表update。
    // 如果当前行已经被其他事务锁住了那么返回一个非Transaction.OPERATION_COMPLETE值表示更新或删除失败了，当前事务要让出当前线程。
    protected int tryUpdateOrRemove(K key, V value, Lockable lockable, boolean isLockedBySelf) {
        transaction.checkNotClosed();
        DataUtils.checkNotNull(lockable, "lockable");
        // 提前调用tryLock的场景直接跳过
        if (!isLockedBySelf && TransactionalValue.tryLock(lockable, transaction) != 1) {
            // 当前行已经被其他事务锁住了
            return Transaction.OPERATION_NEED_WAIT;
        }
        if (lockable.getLock() == null) {
            TransactionalValue.setTransaction(transaction, lockable); // 二级索引需要设置
            if (!markDirtyPage(lockable))
                map.put(key, lockable);
        }
        Object oldValue = lockable.getLockedValue();
        lockable.setLockedValue(value);
        addUndoLog(key, lockable, oldValue);
        return Transaction.OPERATION_COMPLETE;
    }

    @Override
    public int tryLock(Lockable lockable) {
        DataUtils.checkNotNull(lockable, "lockable");
        transaction.checkNotClosed();
        int ret = TransactionalValue.tryLock(lockable, transaction);
        if (ret > 0 && !markDirtyPage(lockable)) {
            ret = -2;
        }
        return ret;
    }

    private static boolean markDirtyPage(Lockable lockable) {
        PageListener oldPageListener = lockable.getPageListener();
        return oldPageListener.getPageReference().markDirtyPage(oldPageListener);
    }

    @Override
    public boolean isLocked(Lockable lockable) {
        return TransactionalValue.isLocked(transaction, lockable.getLock());
    }

    @Override
    public int addWaitingTransaction(Lockable lockable) {
        return TransactionalValue.addWaitingTransaction(lockable, transaction);
    }

    @Override
    public Lockable getLockableValue(K key) {
        return map.get(key);
    }

    @Override
    public Object getOldValue(Lockable lockable) {
        ConcurrentHashMap<Lockable, Object> cache = map.getOldValueCache();
        if (cache != null) {
            OldValue v = (OldValue) cache.get(lockable);
            if (v != null)
                return v.value;
        }
        return null;
    }

    //////////////////// 以下是StorageMap与写操作相关的同步和异步API的实现 ////////////////////////////////

    @Override
    public V put(K key, V value) {
        return put0(transaction.getSession(), key, value, null, false);
    }

    @Override
    public void put(K key, V value, AsyncResultHandler<V> handler) {
        put0(transaction.getSession(), key, value, handler, false);
    }

    @Override
    public void put(InternalSession session, K key, V value, AsyncResultHandler<V> handler) {
        put0(session, key, value, handler, false);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return put0(transaction.getSession(), key, value, null, true);
    }

    @Override
    public void putIfAbsent(K key, V value, AsyncResultHandler<V> handler) {
        put0(transaction.getSession(), key, value, handler, true);
    }

    @Override
    public void putIfAbsent(InternalSession session, K key, V value, AsyncResultHandler<V> handler) {
        put0(session, key, value, handler, true);
    }

    private V put0(InternalSession session, K key, V value, AsyncResultHandler<V> handler,
            boolean ifAbsent) {
        AtomicReference<V> vRef = new AtomicReference<>();
        if (handler != null) {
            put0(session, key, value, handler, ifAbsent, vRef);
        } else {
            SchedulerListener<V> listener = SchedulerListener.createSchedulerListener();
            put0(session, key, value, listener, ifAbsent, vRef);
            listener.await();
        }
        return vRef.get();
    }

    private void put0(InternalSession session, K key, V value, AsyncResultHandler<V> handler,
            boolean ifAbsent, AtomicReference<V> vRef) {
        // 为了支持可重复读事务，还是要读出旧值
        Lockable lockable = map.get(key);
        if (lockable != null) {
            tryUpdateOrRemove(key, value, lockable, handler, vRef);
        } else {
            lockable = toLockable(value);
            add(key, lockable, ifAbsent, vRef, ar -> {
                if (ar.isSucceeded()) {
                    handler.handleResult(vRef.get());
                } else {
                    handler.handleException(ar.getCause());
                }
            });
        }
    }

    private Lockable toLockable(V value) {
        DataUtils.checkNotNull(value, "value");
        Lockable lockable;
        if (value instanceof Lockable)
            lockable = (Lockable) value;
        else
            lockable = new TransactionalValue(value); // 内部没有增加行锁
        return lockable;
    }

    @Override
    public K append(V value) {
        return append0(transaction.getSession(), value, null);
    }

    @Override
    public void append(V value, AsyncResultHandler<K> handler) {
        append0(transaction.getSession(), value, handler);
    }

    @Override
    public void append(InternalSession session, V value, AsyncResultHandler<K> handler) {
        append0(session, value, handler);
    }

    @Override
    public void append(AsyncResultHandler<K> handler, Lockable lockable) {
        append0(transaction.getSession(), lockable, handler);
    }

    private K append0(InternalSession session, V value, AsyncResultHandler<K> handler) {
        Lockable lockable = toLockable(value);
        return append0(session, lockable, handler);
    }

    // 追加新记录时不会产生事务冲突
    private K append0(InternalSession session, Lockable lockable, AsyncResultHandler<K> handler) {
        boolean isUndoLogEnabled = (session == null || session.isUndoLogEnabled());
        if (isUndoLogEnabled)
            TransactionalValue.insertLock(lockable, transaction); // 内部有增加行锁
        if (handler != null) {
            map.append(session, lockable, ar -> {
                if (isUndoLogEnabled && ar.isSucceeded()) {
                    addUndoLog(ar.getResult(), lockable, null);
                }
                handler.handle(ar);
            });
            return null;
        } else {
            K key = map.append(lockable);
            // 记事务log和append新值都是更新内存中的相应数据结构，所以不必把log调用放在append前面
            // 放在前面的话调用log方法时就不知道key是什么，当事务要rollback时就不知道如何修改map的内存数据
            if (isUndoLogEnabled) {
                addUndoLog(key, lockable, null);
            }
            return key;
        }
    }

    private UndoLogRecord addUndoLog(Object key, Lockable lockable, Object oldValue) {
        return transaction.undoLog.add(map, key, lockable, oldValue);
    }

    @Override
    public V remove(K key) {
        return remove0(key, null);
    }

    @Override
    public void remove(K key, AsyncResultHandler<V> handler) {
        remove0(key, handler);
    }

    @Override
    public void remove(InternalSession session, K key, AsyncResultHandler<V> handler) {
        remove0(key, handler);
    }

    private V remove0(K key, AsyncResultHandler<V> handler) {
        AtomicReference<V> vRef = new AtomicReference<>();
        Lockable lockable = map.get(key);
        if (lockable == null) {
            if (handler != null)
                handler.handleResult(null);
        } else {
            if (handler != null) {
                tryUpdateOrRemove(key, null, lockable, handler, vRef);
            } else {
                SchedulerListener<V> listener = SchedulerListener.createSchedulerListener();
                tryUpdateOrRemove(key, null, lockable, listener, vRef);
                listener.await();
            }
        }
        return vRef.get();
    }

    private void tryUpdateOrRemove(K key, V value, Lockable lockable, AsyncResultHandler<V> handler,
            AtomicReference<V> vRef) {
        // tryUpdateOrRemove可能会改变oldValue的内部状态，所以提前拿到返回值
        V oldValue = getUnwrapValue(key, lockable);
        vRef.set(oldValue);
        if (tryUpdateOrRemove(key, value, lockable, false) == Transaction.OPERATION_COMPLETE)
            handler.handleResult(oldValue);
        else
            handler.handleException(DataUtils
                    .newIllegalStateException(DataUtils.ERROR_TRANSACTION_LOCKED, "Entry is locked"));
    }
}
