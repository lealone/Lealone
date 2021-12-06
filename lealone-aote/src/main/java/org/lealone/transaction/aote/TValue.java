/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

public class TValue implements TransactionalValue {

    private static final AtomicReferenceFieldUpdater<TValue, AMTransaction> //
    tUpdater = AtomicReferenceFieldUpdater.newUpdater(TValue.class, AMTransaction.class, "t");

    Object value;
    private volatile AMTransaction t;

    public TValue(Object value) {
        this.value = value;
    }

    public TValue(Object value, AMTransaction t) {
        this.value = value;
        this.t = t;
    }

    @Override
    public boolean tryLock(AMTransaction t, int[] columnIndexes) {
        if (t == this.t)
            return true;
        return tUpdater.compareAndSet(this, null, t);
    }

    @Override
    public void unlock() {
        t = null;
    }

    public AMTransaction getTransaction() {
        return t;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
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

    @Override
    public void setValue(Object value) {
        this.value = value;
    }

    // 如果是0代表事务已经提交，对于已提交事务，只有在写入时才写入tid=0，
    // 读出来的时候为了不占用内存就不加tid字段了，这样每条已提交记录能省8个字节(long)的内存空间
    @Override
    public long getTid() {
        return 0;
    }

    @Override
    public int getLogId() {
        return 0;
    }

    @Override
    public boolean supportsColumnLock() {
        return false;
    }

    @Override
    public boolean isLocked(long tid, int[] columnIndexes) {
        return false;
    }

    @Override
    public AMTransaction getLockOwner(int[] columnIndexes) {
        return t;
    }

    @Override
    public String getHostAndPort() {
        return null;
    }

    @Override
    public String getGlobalReplicationName() {
        return null;
    }

    @Override
    public boolean isReplicated() {
        return false;
    }

    @Override
    public void setReplicated(boolean replicated) {
    }

    @Override
    public void incrementVersion() {
    }

    @Override
    public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key) {
        return this;
    }

    @Override
    public TransactionalValue getCommitted(AMTransaction transaction) {
        return this;
    }

    @Override
    public TransactionalValue commit(long tid) {
        return this;
    }

    @Override
    public boolean isCommitted() {
        return true;
    }

    @Override
    public void rollback() {
    }

    @Override
    public void write(DataBuffer buff, StorageDataType valueType) {
        writeMeta(buff);
        writeValue(buff, valueType);
    }

    @Override
    public void writeValue(DataBuffer buff, StorageDataType valueType) {
        if (value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, value);
        }
    }

    @Override
    public TransactionalValue getOldValue() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setOldValue(TransactionalValue oldValue) {
        // TODO Auto-generated method stub
    }

    @Override
    public void writeMeta(DataBuffer buff) {
        // TODO Auto-generated method stub
    }

    @Override
    public String toString() {
        AMTransaction t = this.t;
        StringBuilder buff = new StringBuilder(getClass().getSimpleName() + "[ ");
        buff.append("tid = ").append(t == null ? 0 : t.getTransactionId());
        buff.append(", value = ").append(value);
        // buff.append(", oldValue = ").append(oldValue);
        buff.append(" ]");
        return buff.toString();
    }

    // static Uncommitted read(long tid, StorageDataType valueType, ByteBuffer buff, StorageDataType oldValueType) {
    // return read(tid, valueType, buff, oldValueType, false, 0);
    // }
    //
    // static Uncommitted readMeta(long tid, StorageDataType valueType, ByteBuffer buff, StorageDataType oldValueType,
    // int columnCount) {
    // return read(tid, valueType, buff, oldValueType, true, columnCount);
    // }
    //
    // private static Uncommitted read(long tid, StorageDataType valueType, ByteBuffer buff, StorageDataType
    // oldValueType,
    // boolean meta, int columnCount) {
    // int logId = DataUtils.readVarInt(buff);
    // int[] columnIndexes = null;
    // boolean rowLock = buff.get() == 0;
    // if (!rowLock) {
    // int len = DataUtils.readVarInt(buff);
    // columnIndexes = new int[len];
    // for (int i = 0; i < len; i++) {
    // columnIndexes[i] = DataUtils.readVarInt(buff);
    // }
    // }
    // TransactionalValue oldValue = null;
    // if (buff.get() == 1) {
    // oldValue = (TransactionalValue) oldValueType.read(buff);
    // }
    // String hostAndPort = ValueString.type.read(buff);
    // String globalReplicationName = ValueString.type.read(buff);
    // long version = DataUtils.readVarLong(buff);
    // Object value;
    // if (meta)
    // value = valueType.readMeta(buff, columnCount);
    // else
    // value = TransactionalValue.readValue(buff, valueType);
    // Uncommitted uncommitted;
    // if (rowLock) {
    // uncommitted = new UncRowLock(tid, value, logId, oldValue, oldValueType, hostAndPort, globalReplicationName,
    // version);
    // } else {
    // uncommitted = new UncColumnLock(tid, value, logId, oldValue, oldValueType, hostAndPort,
    // globalReplicationName, version, columnIndexes);
    // }
    // return uncommitted;
    // }
    //
    // @Override
    // public void writeMeta(DataBuffer buff) {
    // buff.putVarLong(tid);
    // buff.putVarInt(logId);
    // if (isRowLock()) {
    // buff.put((byte) 0);
    // } else {
    // buff.put((byte) 1);
    // int[] columnIndexes = getColumnIndexes();
    // int len = columnIndexes.length;
    // buff.putVarInt(len);
    // for (int i = 0; i < len; i++) {
    // buff.putVarInt(columnIndexes[i]);
    // }
    // }
    // if (oldValue == null) {
    // buff.put((byte) 0);
    // } else {
    // buff.put((byte) 1);
    // oldValueType.write(buff, oldValue);
    // }
    // ValueString.type.write(buff, hostAndPort);
    // ValueString.type.write(buff, globalReplicationName);
    // buff.putVarLong(version);
    // }

}
