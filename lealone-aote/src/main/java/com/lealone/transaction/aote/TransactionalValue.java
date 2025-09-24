/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.lock.Lock;
import com.lealone.db.lock.LockOwner;
import com.lealone.db.lock.Lockable;
import com.lealone.db.lock.LockableBase;
import com.lealone.storage.StorageMap;
import com.lealone.storage.type.StorageDataType;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.aote.lock.RowLock;

//每个表的每一条记录都对应这个类的一个实例，所以不能随意在这个类中加新的字段，否则会占用很多内存
public class TransactionalValue extends LockableBase {

    public static class OldValue {
        final long tid;
        final Object key;
        final Object value;
        OldValue next;
        boolean useLast;

        public OldValue(long tid, Object key, Object value) {
            this.tid = tid;
            this.key = key;
            this.value = value;
        }
    }

    // 对于一个已经提交的值，如果当前事务因为隔离级别的原因读不到这个值，那么就返回SIGHTLESS
    public static final Object SIGHTLESS = new Object();

    private Object value;

    public TransactionalValue(Object value) {
        this.value = value;
    }

    @Override
    public void setLockedValue(Object value) {
        this.value = value;
    }

    @Override
    public Object getLockedValue() {
        return value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public Object copy(Object oldLockedValue, Lock lock) {
        return oldLockedValue;
    }

    public boolean isCommitted() {
        return isCommitted(this);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder("TV[ ");
        buff.append("tid = ").append(getTid(this));
        buff.append(", value = ").append(value);
        buff.append(" ]");
        return buff.toString();
    }

    public static void insertLock(Lockable Lockable, AOTransaction t) {
        RowLock rowLock = new RowLock(Lockable);
        Lockable.setLock(rowLock);
        rowLock.lockFast(t, Lockable, null); // insert的场景，old value是null
    }

    // 二级索引需要设置
    public static void setTransaction(AOTransaction t, Lockable lockable) {
        Lock lock = getOrSetLock(lockable);
        if (lock.getTransaction() == null)
            lock.tryLock(t, lockable, lockable.getLockedValue());
    }

    private static Lock getOrSetLock(Lockable lockable) {
        Lock old = lockable.getLock();
        // 加一个if判断，避免创建对象
        if (old == null) {
            lockable.compareAndSetLock(null, new RowLock(lockable));
        } else if (old.isPageLock()) {
            RowLock newLock = new RowLock(lockable);
            newLock.setPageListener(old.getPageListener());
            lockable.compareAndSetLock(old, newLock);
        }
        return lockable.getLock();
    }

    public static Object getOldValue(Lockable lockable) {
        Lock lock = lockable.getLock();
        return lock != null ? lock.getOldValue() : null;
    }

    public static Object getValue(Lockable lockable, AOTransaction transaction, StorageMap<?, ?> map) {
        // 如果事务当前执行的是更新类的语句那么自动通过READ_COMMITTED级别读取最新版本的记录
        int isolationLevel = transaction.isUpdateCommand() ? Transaction.IL_READ_COMMITTED
                : transaction.getIsolationLevel();

        Lock lock = lockable.getLock();
        LockOwner lockOwner;
        AOTransaction t;
        if (lock == null) { // 如果没有锁了也不能立刻返回，要考虑可重复读的场景
            lockOwner = null;
            t = null;
        } else {
            if (lock.isPageLock() && isolationLevel < Transaction.IL_REPEATABLE_READ) {
                if (lockable.getLockedValue() == null)
                    return null; // 已经删除
                else
                    return lockable.getValue();
            }
            // 先拿到LockOwner再用它获取信息，否则会产生并发问题
            lockOwner = lock.getLockOwner();
            t = (AOTransaction) lockOwner.getTransaction();
            // 如果拥有锁的事务是当前事务或当前事务的父事务，直接返回当前值
            if (t != null && (t == transaction || t == transaction.getParentTransaction())) {
                if (lockable.getLockedValue() == null)
                    return null; // 已经删除
                else
                    return lockable.getValue();
            }
        }
        switch (isolationLevel) {
        case Transaction.IL_READ_COMMITTED: {
            if (t == null) {
                return lockable.getValue();
            }
            if (t.isCommitted()) {
                return getValue(lockable);
            } else {
                if (lockOwner.getOldValue() == null)
                    return SIGHTLESS; // 刚刚insert但是还没有提交的记录
                else
                    return lockable.copy(lockOwner.getOldValue(), lock);
            }
        }
        case Transaction.IL_REPEATABLE_READ:
        case Transaction.IL_SERIALIZABLE: {
            long tid = transaction.getTransactionId();
            if (t != null && t.commitTimestamp > 0 && tid >= t.commitTimestamp) {
                return getValue(lockable);
            }
            ConcurrentHashMap<Lockable, Object> oldValueCache = map.getOldValueCache();
            OldValue oldValue = (OldValue) oldValueCache.get(lockable);
            if (oldValue != null) {
                if (tid >= oldValue.tid) {
                    if (t != null && lockOwner.getOldValue() != null)
                        return lockable.copy(lockOwner.getOldValue(), lock);
                    else
                        return lockable.getValue();
                }
                while (oldValue != null) {
                    if (tid >= oldValue.tid)
                        return lockable.copy(oldValue.value, lock);
                    oldValue = oldValue.next;
                }
                return SIGHTLESS; // insert成功后的记录，旧事务看不到
            }
            if (t != null) {
                if (lockOwner.getOldValue() != null)
                    return lockable.copy(lockOwner.getOldValue(), lock);
                else
                    return SIGHTLESS; // 刚刚insert但是还没有提交的记录
            } else {
                return getValue(lockable);
            }
        }
        case Transaction.IL_READ_UNCOMMITTED: {
            return getValue(lockable);
        }
        default:
            throw DbException.getInternalError();
        }
    }

    private static Object getValue(Lockable lockable) {
        // 已经删除
        if (lockable.getLockedValue() == null)
            return null;
        else
            return lockable.getValue();
    }

    // 如果是0代表事务已经提交，对于已提交事务，只有在写入时才写入tid=0，
    // 读出来的时候为了不占用内存就不加tid字段了，这样每条已提交记录能省8个字节(long)的内存空间
    public static long getTid(Lockable lockable) {
        Lock lock = lockable.getLock();
        if (lock == null)
            return 0;
        Transaction t = lock.getTransaction();
        return t == null ? 0 : t.getTransactionId();
    }

    // 小于0：已经删除
    // 等于0：加锁失败
    // 大于0：加锁成功
    public static int tryLock(Lockable lockable, AOTransaction t) {
        while (true) {
            Lock old = getOrSetLock(lockable);
            if (lockable.isNoneLock()) // 上一个事务替换为NULL时重试
                continue;
            RowLock rowLock = (RowLock) old;
            Object value = lockable.getLockedValue();
            if (value == null && !isLocked(t, rowLock)) // 已经删除了
                return -1;
            if (rowLock.tryLock(t, lockable, value)) {
                // 锁到旧的了，要重试
                if (rowLock != lockable.getLock()) {
                    rowLock.removeLock(t);
                    rowLock.unlockFast();
                    continue;
                }
                if (value == null) // 已经删除了
                    return -1;
                else
                    return 1;
            }
            return 0;
        }
    }

    public static boolean isLocked(AOTransaction t, Lock lock) {
        if (lock == null || lock.getTransaction() == null)
            return false;
        else
            return lock.getTransaction() != t;
    }

    public static int addWaitingTransaction(Lockable lockable, AOTransaction t) {
        Lock lock = lockable.getLock();
        if (lock == null)
            return Transaction.OPERATION_NEED_RETRY;
        return lock.addWaitingTransaction(lockable, lock.getTransaction(), t.getSession());
    }

    public static void commit(boolean isInsert, StorageMap<?, ?> map, Object key, Lockable lockable) {
        if (lockable.isNoneLock())
            return;
        RowLock rowLock = (RowLock) lockable.getLock();
        AOTransaction t = rowLock.getTransaction();
        if (t == null)
            return;
        Object value = lockable.getLockedValue();
        AOTransactionEngine te = t.transactionEngine;
        if (te.containsRepeatableReadTransactions()) {
            // 如果parent不为null就用parent的commitTimestamp，比如执行异步索引操作时就要用parent的commitTimestamp
            Transaction parent = t.getParentTransaction();
            long commitTimestamp = parent != null ? parent.getCommitTimestamp() : t.commitTimestamp;
            ConcurrentHashMap<Lockable, Object> oldValueCache = map.getOldValueCache();
            if (isInsert) {
                OldValue v = new OldValue(commitTimestamp, key, value);
                oldValueCache.put(lockable, v);
            } else {
                long maxTid = te.getMaxRepeatableReadTransactionId();
                OldValue old = (OldValue) oldValueCache.get(lockable);
                // 如果现有的版本已经足够给所有的可重复读事务使用了，那就不再加了
                if (old != null && old.tid > maxTid) {
                    old.useLast = true;
                    return;
                }
                OldValue v = new OldValue(commitTimestamp, key, rowLock.getOldValue());
                if (old == null) {
                    OldValue ov = new OldValue(0, key, rowLock.getOldValue());
                    v.next = ov;
                } else if (old.useLast) {
                    OldValue ov = new OldValue(old.tid + 1, key, rowLock.getOldValue());
                    ov.next = old;
                    v.next = ov;
                } else {
                    v.next = old;
                }
                oldValueCache.put(lockable, v);
            }
        }
    }

    public static boolean isCommitted(Lockable lockable) {
        Lock lock = lockable.getLock();
        if (lock == null)
            return true;
        AOTransaction t = (AOTransaction) lock.getTransaction();
        return t == null || t.isCommitted();
    }

    public static void rollback(Object oldValue, Lockable lockable) {
        lockable.setLockedValue(oldValue);
    }

    public static void write(Lockable lockable, DataBuffer buff, StorageDataType valueType,
            boolean isByteStorage) {
        writeMeta(lockable, buff);
        writeValue(lockable, buff, valueType, isByteStorage);
    }

    public static void writeMeta(Lockable lockable, DataBuffer buff) {
        // AOTransaction t = rowLock.getTransaction();
        // if (t == null) {
        // buff.putVarLong(0);
        // } else {
        // buff.putVarLong(t.transactionId);
        // }
        buff.putVarLong(0); // 兼容老版本
    }

    private static void writeValue(Lockable lockable, DataBuffer buff, StorageDataType valueType,
            boolean isByteStorage) {
        // 一些存储引擎写入key和value前都需要事先转成字节数组，所以需要先写未提交的数据
        Object value = isByteStorage ? lockable.getValue() : getCommittedValue(lockable);
        if (value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, value);
        }
    }

    private static Object getCommittedValue(Lockable lockable) {
        return lockable.getValue();
        // RowLock rowLock = (RowLock) lockable.getLock();
        // if (rowLock == null)
        // return lockable.getValue();
        // Object oldValue = rowLock.getOldValue();
        // AOTransaction t = rowLock.getTransaction();
        // if (oldValue == null || t == null || t.commitTimestamp > 0)
        // return lockable.getValue();
        // else
        // return lockable.copy(oldValue, rowLock);
    }

    public static Lockable readMeta(ByteBuffer buff, StorageDataType valueType,
            StorageDataType oldValueType, Object obj, int columnCount) {
        DataUtils.readVarLong(buff); // 忽略tid
        Object value = valueType.readMeta(buff, obj, columnCount);
        return createCommitted(value);
    }

    public static Lockable read(ByteBuffer buff, StorageDataType valueType,
            StorageDataType oldValueType) {
        DataUtils.readVarLong(buff); // 忽略tid
        Object value = readValue(buff, valueType);
        return createCommitted(value);
    }

    private static Object readValue(ByteBuffer buff, StorageDataType valueType) {
        if (buff.get() == 1)
            return valueType.read(buff);
        else
            return null;
    }

    public static Lockable createCommitted(Object value) {
        if (value instanceof Lockable) {
            return (Lockable) value;
        } else {
            return new TransactionalValue(value);
        }
    }
}
