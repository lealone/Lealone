/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

public class TransactionalValue {

    // 对于一个已经提交的值，如果当前事务因为隔离级别的原因读不到这个值，那么就返回SIGHTLESS
    public static final TransactionalValue SIGHTLESS = createCommitted(null);
    private static final AtomicReferenceFieldUpdater<TransactionalValue, AMTransaction> //
    tUpdater = AtomicReferenceFieldUpdater.newUpdater(TransactionalValue.class, AMTransaction.class, "t");

    private Object value;
    private volatile AMTransaction t;

    public TransactionalValue(Object value) {
        this.value = value;
    }

    public TransactionalValue(Object value, AMTransaction t) {
        this.value = value;
        this.t = t;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public Object getValue(AMTransaction transaction) {
        return value;
        // AMTransaction t = this.t;
        // if (t.isCommitted()) {
        // switch (transaction.getIsolationLevel()) {
        // case Transaction.IL_REPEATABLE_READ:
        // case Transaction.IL_SERIALIZABLE:
        // // if (transaction.getTransactionId() >= t)
        // // return this;
        // // else if (oldValue != null) {
        // // return oldValue.getCommitted(transaction);
        // // }
        // return SIGHTLESS;
        // default:
        // return this;
        // }
        // } else {
        // if (transaction.getIsolationLevel() == Transaction.IL_READ_UNCOMMITTED) {
        // return this;
        // } else if (oldValue != null) {
        // return oldValue.getCommitted(transaction);
        // } else {
        // return null;
        // }
        // }
        // return value;
    }

    public TransactionalValue getOldValue() {
        return null;
    }

    public void setOldValue(TransactionalValue oldValue) {
    }

    // 如果是0代表事务已经提交，对于已提交事务，只有在写入时才写入tid=0，
    // 读出来的时候为了不占用内存就不加tid字段了，这样每条已提交记录能省8个字节(long)的内存空间
    public long getTid() {
        AMTransaction t = this.t;
        return t == null ? 0 : t.transactionId;
    }

    public int getLogId() {
        AMTransaction t = this.t;
        return t == null ? 0 : t.getUndoLog().getLogId();
    }

    public boolean supportsColumnLock() {
        return false;
    }

    public boolean tryLock(AMTransaction t, int[] columnIndexes) {
        if (t == this.t)
            return true;
        return tUpdater.compareAndSet(this, null, t);
    }

    public void unlock() {
        t = null;
    }

    public boolean isLocked(long tid, int[] columnIndexes) {
        return false;
    }

    public AMTransaction getLockOwner(int[] columnIndexes) {
        return t;
    }

    public String getHostAndPort() {
        return null;
    }

    public String getGlobalReplicationName() {
        return null;
    }

    public boolean isReplicated() {
        return false;
    }

    public void setReplicated(boolean replicated) {
    }

    public void incrementVersion() {
    }

    public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key) {
        return this;
    }

    public TransactionalValue getCommitted(AMTransaction transaction) {
        return this;
    }

    public TransactionalValue commit(long tid) {
        return this;
    }

    public boolean isCommitted() {
        return true;
    }

    public void rollback() {
    }

    public void write(DataBuffer buff, StorageDataType valueType) {
        writeMeta(buff);
        writeValue(buff, valueType);
    }

    public void writeMeta(DataBuffer buff) {
        AMTransaction t = this.t;
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

    public List<String> getRetryReplicationNames() {
        return null;
    }

    public void setRetryReplicationNames(List<String> retryReplicationNames) {
    }

    public void gc(AMTransaction transaction) {
    }

    public static TransactionalValue readMeta(ByteBuffer buff, StorageDataType valueType, StorageDataType oldValueType,
            int columnCount) {
        long tid = DataUtils.readVarLong(buff);
        if (tid == 0) {
            Object value = valueType.readMeta(buff, columnCount);
            return TransactionalValue.createCommitted(value);
        } else {
            // TODO 有没有必要写未提交的事务
            Object value = valueType.readMeta(buff, columnCount);
            return TransactionalValue.createCommitted(value);
        }
    }

    public static Object readValue(ByteBuffer buff, StorageDataType valueType) {
        Object value = null;
        if (buff.get() == 1) {
            value = valueType.read(buff);
        }
        return value;
    }

    public static TransactionalValue read(ByteBuffer buff, StorageDataType valueType, StorageDataType oldValueType) {
        long tid = DataUtils.readVarLong(buff);
        if (tid == 0) {
            Object value = TransactionalValue.readValue(buff, valueType);
            return TransactionalValue.createCommitted(value);
        } else {
            // TODO 有没有必要写未提交的事务
            Object value = TransactionalValue.readValue(buff, valueType);
            return TransactionalValue.createCommitted(value);
        }
    }

    public static TransactionalValue createCommitted(Object value) {
        return new TransactionalValue(value);
    }
}
