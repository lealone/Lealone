/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;

//每个表的每一条记录都对应这个类的一个实例，所以不能随意在这个类中加新的字段
public class TransactionalValue {

    public static class LockOwner {
        int logId;
        Object oldValue;
    }

    public static class OldValue {
        long tid;
        Object value;
        OldValue next;
    }

    // 对于一个已经提交的值，如果当前事务因为隔离级别的原因读不到这个值，那么就返回SIGHTLESS
    public static final Object SIGHTLESS = new Object();

    private static final AtomicReferenceFieldUpdater<TransactionalValue, AOTransaction> tUpdater = //
            AtomicReferenceFieldUpdater.newUpdater(TransactionalValue.class, AOTransaction.class, "t");

    private Object value;
    private volatile AOTransaction t;

    public TransactionalValue(Object value) {
        this.value = value;
    }

    public TransactionalValue(Object value, AOTransaction t) {
        this.value = value;
        this.t = t;
    }

    private void addLockOwner(AOTransaction t) {
        LockOwner owner = new LockOwner();
        owner.logId = t.getUndoLog().getLogId();
        owner.oldValue = value;
        t.addTransactionalValue(this, owner);
    }

    // 二级索引需要设置
    public void setTransaction(AOTransaction t) {
        if (this.t == null) {
            addLockOwner(t);
            this.t = t;
        }
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public Object getValue(AOTransaction transaction) {
        AOTransaction t = this.t;
        if (t == transaction)
            return value;
        switch (transaction.getIsolationLevel()) {
        case Transaction.IL_READ_COMMITTED: {
            if (t != null) {
                if (t.isCommitted()) {
                    return value;
                } else {
                    LockOwner owner = t.getLockOwner(this);
                    if (owner == null)
                        return SIGHTLESS; // 刚刚insert但是还没有提交的记录
                    else
                        return owner.oldValue;
                }
            }
            return value;
        }
        case Transaction.IL_REPEATABLE_READ:
        case Transaction.IL_SERIALIZABLE: {
            long tid = transaction.getTransactionId();
            if (t != null) {
                if (t.isCommitted() && tid >= t.commitTimestamp)
                    return value;
            }
            OldValue oldValue = transaction.transactionEngine.getOldValue(this);
            boolean hasOld = oldValue != null;
            while (oldValue != null) {
                if (tid >= oldValue.tid)
                    return oldValue.value;
                oldValue = oldValue.next;
            }
            if (hasOld) {
                return SIGHTLESS; // insert成功后的记录，旧事务看不到
            }
            if (t != null) {
                LockOwner owner = t.getLockOwner(this);
                if (owner != null)
                    return owner.oldValue;
            } else {
                return value;
            }
            return SIGHTLESS; // 刚刚insert但是还没有提交的记录
        }
        case Transaction.IL_READ_UNCOMMITTED: {
            return value;
        }
        default:
            throw DbException.getInternalError();
        }
    }

    // 如果是0代表事务已经提交，对于已提交事务，只有在写入时才写入tid=0，
    // 读出来的时候为了不占用内存就不加tid字段了，这样每条已提交记录能省8个字节(long)的内存空间
    public long getTid() {
        AOTransaction t = this.t;
        return t == null ? 0 : t.transactionId;
    }

    public int getLogId() {
        AOTransaction t = this.t;
        if (t != null) {
            LockOwner owner = t.getLockOwner(this);
            if (owner != null)
                return owner.logId;
        }
        return 0;
    }

    public boolean tryLock(AOTransaction t, int[] columnIndexes) {
        if (t == this.t)
            return true;
        addLockOwner(t);
        boolean ok = tUpdater.compareAndSet(this, null, t);
        if (!ok) {
            t.removeTransactionalValue(this);
        }
        return ok;
    }

    public void unlock(boolean isInsert) {
        AOTransaction t = this.t;
        if (t == null)
            return;
        this.t = null;
        if (!isInsert)
            t.removeTransactionalValue(this);
    }

    public boolean isLocked(long tid, int[] columnIndexes) {
        AOTransaction t = this.t;
        return t == null ? false : t.transactionId != tid;
    }

    public AOTransaction getLockOwner(int[] columnIndexes) {
        return t;
    }

    public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key) {
        return this;
    }

    public void commit(boolean isInsert) {
        AOTransaction t = this.t;
        if (t == null)
            return;
        if (t.transactionEngine.containsRepeatableReadTransactions()) {
            synchronized (this) {
                OldValue v = new OldValue();
                if (!isInsert) {
                    v.next = t.transactionEngine.getOldValue(this);
                    if (v.next == null) {
                        OldValue ov = new OldValue();
                        LockOwner owner = t.getLockOwner(this);
                        if (owner != null)
                            ov.value = owner.oldValue;
                        v.next = ov;
                    }
                }
                v.value = value;
                v.tid = t.commitTimestamp;
                t.transactionEngine.addTransactionalValue(this, v);
            }
        }
    }

    public boolean isCommitted() {
        AOTransaction t = this.t;
        return t == null || t.isCommitted();
    }

    public void rollback(Object oldValue) {
        this.value = oldValue;
    }

    public void write(DataBuffer buff, StorageDataType valueType) {
        writeMeta(buff);
        writeValue(buff, valueType);
    }

    public void writeMeta(DataBuffer buff) {
        AOTransaction t = this.t;
        if (t == null) {
            buff.putVarLong(0);
        } else {
            buff.putVarLong(t.transactionId);
        }
    }

    private void writeValue(DataBuffer buff, StorageDataType valueType) {
        if (value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, value);
        }
    }

    public static TransactionalValue readMeta(ByteBuffer buff, StorageDataType valueType,
            StorageDataType oldValueType, int columnCount) {
        long tid = DataUtils.readVarLong(buff);
        Object value = valueType.readMeta(buff, columnCount);
        return create(tid, value);
    }

    public static TransactionalValue read(ByteBuffer buff, StorageDataType valueType,
            StorageDataType oldValueType) {
        long tid = DataUtils.readVarLong(buff);
        Object value = readValue(buff, valueType);
        return create(tid, value);
    }

    private static Object readValue(ByteBuffer buff, StorageDataType valueType) {
        if (buff.get() == 1)
            return valueType.read(buff);
        else
            return null;
    }

    private static TransactionalValue create(long tid, Object value) {
        if (tid == 0) {
            return createCommitted(value);
        } else {
            // TODO 有没有必要写未提交的事务
            return createCommitted(value);
        }
    }

    public static TransactionalValue createCommitted(Object value) {
        return new TransactionalValue(value);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder("TV[ ");
        buff.append("tid = ").append(getTid());
        buff.append(", value = ").append(value);
        buff.append(" ]");
        return buff.toString();
    }
}
