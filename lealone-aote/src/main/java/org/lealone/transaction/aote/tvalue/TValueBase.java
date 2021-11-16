/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tvalue;

import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.aote.AMTransaction;

abstract class TValueBase implements TransactionalValue {

    public final Object value;

    public TValueBase(Object value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public boolean compareAndSet(TransactionalValue expect, TransactionalValue update) {
        return true;
    }

    @Override
    public TransactionalValue getRef() {
        return null;
    }

    @Override
    public TransactionalValue getRefValue() {
        return this;
    }

    @Override
    public void setRefValue(TransactionalValue v) {
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
    public boolean isLocked(long tid, int[] columnIndexes) {
        return false;
    }

    @Override
    public long getLockOwnerTid(long tid, int[] columnIndexes) {
        return -1;
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
    public TransactionalValue getCommitted() {
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
    public TransactionalValue remove(long tid) {
        return this;
    }

    @Override
    public void write(DataBuffer buff, StorageDataType valueType) {
        writeMeta(buff);
        writeValue(buff, valueType);
    }

    @Override
    public abstract void writeMeta(DataBuffer buff);

    @Override
    public void writeValue(DataBuffer buff, StorageDataType valueType) {
        if (value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, value);
        }
    }
}
