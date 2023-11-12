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
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.ITransactionalValue;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.aote.lock.InsertRowLock;
import org.lealone.transaction.aote.lock.RowLock;

//每个表的每一条记录都对应这个类的一个实例，所以不能随意在这个类中加新的字段
public class TransactionalValue implements ITransactionalValue {

    public static class OldValue {
        final long tid;
        final Object value;
        OldValue next;
        boolean useLast;

        public OldValue(long tid, Object value) {
            this.tid = tid;
            this.value = value;
        }
    }

    // 对于一个已经提交的值，如果当前事务因为隔离级别的原因读不到这个值，那么就返回SIGHTLESS
    public static final Object SIGHTLESS = new Object();

    private static final AtomicReferenceFieldUpdater<TransactionalValue, RowLock> rowLockUpdater = //
            AtomicReferenceFieldUpdater.newUpdater(TransactionalValue.class, RowLock.class, "rowLock");

    private static final RowLock NULL = new RowLock();

    private volatile RowLock rowLock = NULL;
    private Object value;

    public TransactionalValue(Object value) {
        this.value = value;
    }

    public TransactionalValue(Object value, AOTransaction t) {
        this.value = value;
        rowLock = new InsertRowLock(this);
        rowLock.tryLock(t, this, null); // insert的场景，old value是null
    }

    public void resetRowLock() {
        rowLock = NULL;
    }

    // 二级索引需要设置
    public void setTransaction(AOTransaction t) {
        if (rowLock == NULL) {
            rowLockUpdater.compareAndSet(this, NULL, new InsertRowLock(this));
        }
        if (rowLock.getTransaction() == null)
            rowLock.tryLock(t, this, value);
    }

    public AOTransaction getTransaction() {
        return rowLock.getTransaction();
    }

    Object getOldValue() {
        return rowLock.getOldValue();
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    public Object getValue(AOTransaction transaction) {
        AOTransaction t = rowLock.getTransaction();
        if (t == transaction)
            return value;
        // 如果事务当前执行的是更新类的语句那么自动通过READ_COMMITTED级别读取最新版本的记录
        int isolationLevel = transaction.isUpdateCommand() ? Transaction.IL_READ_COMMITTED
                : transaction.getIsolationLevel();
        switch (isolationLevel) {
        case Transaction.IL_READ_COMMITTED: {
            if (t != null) {
                if (t.isCommitted()) {
                    return value;
                } else {
                    if (rowLock.getOldValue() == null)
                        return SIGHTLESS; // 刚刚insert但是还没有提交的记录
                    else
                        return rowLock.getOldValue();
                }
            }
            return value;
        }
        case Transaction.IL_REPEATABLE_READ:
        case Transaction.IL_SERIALIZABLE: {
            long tid = transaction.getTransactionId();
            if (t != null && t.commitTimestamp > 0 && tid >= t.commitTimestamp) {
                return value;
            }
            OldValue oldValue = transaction.transactionEngine.getOldValue(this);
            if (oldValue != null) {
                if (tid >= oldValue.tid) {
                    if (t != null && rowLock.getOldValue() != null)
                        return rowLock.getOldValue();
                    else
                        return value;
                }
                while (oldValue != null) {
                    if (tid >= oldValue.tid)
                        return oldValue.value;
                    oldValue = oldValue.next;
                }
                return SIGHTLESS; // insert成功后的记录，旧事务看不到
            }
            if (t != null) {
                if (rowLock.getOldValue() != null)
                    return rowLock.getOldValue();
                else
                    return SIGHTLESS; // 刚刚insert但是还没有提交的记录
            } else {
                return value;
            }
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
        AOTransaction t = rowLock.getTransaction();
        return t == null ? 0 : t.transactionId;
    }

    // 小于0：已经删除
    // 等于0：加锁失败
    // 大于0：加锁成功
    public int tryLock(AOTransaction t) {
        // 加一个if判断，避免创建对象
        if (rowLock == NULL) {
            rowLockUpdater.compareAndSet(this, NULL, new RowLock());
        }
        if (value == null && !isLocked(t)) // 已经删除了
            return -1;
        if (rowLock.tryLock(t, this, value))
            if (value == null) // 已经删除了
                return -1;
            else
                return 1;
        else
            return 0;
    }

    public boolean isLocked(AOTransaction t) {
        if (rowLock.getTransaction() == null)
            return false;
        else
            return rowLock.getTransaction() != t;
    }

    public int addWaitingTransaction(Object key, AOTransaction t) {
        return rowLock.addWaitingTransaction(key, rowLock.getTransaction(), t.getSession());
    }

    public void commit(boolean isInsert) {
        AOTransaction t = rowLock.getTransaction();
        if (t == null)
            return;
        AOTransactionEngine te = t.transactionEngine;
        if (te.containsRepeatableReadTransactions()) {
            if (isInsert) {
                OldValue v = new OldValue(t.commitTimestamp, value);
                te.addTransactionalValue(this, v);
            } else {
                long maxTid = te.getMaxRepeatableReadTransactionId();
                OldValue old = te.getOldValue(this);
                // 如果现有的版本已经足够给所有的可重复读事务使用了，那就不再加了
                if (old != null && old.tid > maxTid) {
                    old.useLast = true;
                    return;
                }
                OldValue v = new OldValue(t.commitTimestamp, value);
                if (old == null) {
                    OldValue ov = new OldValue(0, rowLock.getOldValue());
                    v.next = ov;
                } else if (old.useLast) {
                    OldValue ov = new OldValue(old.tid + 1, rowLock.getOldValue());
                    ov.next = old;
                    v.next = ov;
                } else {
                    v.next = old;
                }
                te.addTransactionalValue(this, v);
            }
        }
    }

    public boolean isCommitted() {
        AOTransaction t = rowLock.getTransaction();
        return t == null || t.isCommitted();
    }

    public void rollback(Object oldValue) {
        this.value = oldValue;
    }

    public void write(DataBuffer buff, StorageDataType valueType, boolean isByteStorage) {
        writeMeta(buff);
        writeValue(buff, valueType, isByteStorage);
    }

    public void writeMeta(DataBuffer buff) {
        // AOTransaction t = rowLock.getTransaction();
        // if (t == null) {
        // buff.putVarLong(0);
        // } else {
        // buff.putVarLong(t.transactionId);
        // }
        buff.putVarLong(0); // 兼容老版本
    }

    private void writeValue(DataBuffer buff, StorageDataType valueType, boolean isByteStorage) {
        // 一些存储引擎写入key和value前都需要事先转成字节数组，所以需要先写未提交的数据
        Object value = isByteStorage ? this.value : getCommittedValue();
        if (value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, value);
        }
    }

    private Object getCommittedValue() {
        AOTransaction t = rowLock.getTransaction();
        if (t == null || t.commitTimestamp > 0)
            return value;
        else
            return rowLock.getOldValue();
    }

    public static TransactionalValue readMeta(ByteBuffer buff, StorageDataType valueType,
            StorageDataType oldValueType, int columnCount) {
        DataUtils.readVarLong(buff); // 忽略tid
        Object value = valueType.readMeta(buff, columnCount);
        return createCommitted(value);
    }

    public static TransactionalValue read(ByteBuffer buff, StorageDataType valueType,
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
