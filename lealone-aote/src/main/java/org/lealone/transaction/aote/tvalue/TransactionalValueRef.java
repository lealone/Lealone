/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tvalue;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.aote.AMTransaction;

// 因为每条记录都对应此类的一个实例，所以为了节约内存没有直接使用java.util.concurrent.atomic.AtomicReference
public class TransactionalValueRef implements TransactionalValue {

    private static final AtomicReferenceFieldUpdater<TransactionalValueRef, TransactionalValue> //
    tvUpdater = AtomicReferenceFieldUpdater //
            .newUpdater(TransactionalValueRef.class, TransactionalValue.class, "tv");

    private volatile TransactionalValue tv;
    private List<String> retryReplicationNames;

    TransactionalValueRef(TransactionalValue transactionalValue) {
        tv = transactionalValue;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder("Ref[ ");
        buff.append(tv).append(" ]");
        return buff.toString();
    }

    @Override
    public Object getValue() {
        return tv.getValue();
    }

    @Override
    public TransactionalValue getOldValue() {
        return tv.getOldValue();
    }

    @Override
    public void setOldValue(TransactionalValue oldValue) {
        tv.setOldValue(oldValue);
    }

    @Override
    public boolean compareAndSet(TransactionalValue expect, TransactionalValue update) {
        return tvUpdater.compareAndSet(this, expect, update);
    }

    @Override
    public TransactionalValue getRef() {
        return this;
    }

    @Override
    public TransactionalValue getRefValue() {
        return tv;
    }

    @Override
    public void setRefValue(TransactionalValue v) {
        tv = v;
    }

    @Override
    public long getTid() {
        return tv.getTid();
    }

    @Override
    public int getLogId() {
        return tv.getLogId();
    }

    @Override
    public boolean isLocked(long tid, int[] columnIndexes) {
        return tv.isLocked(tid, columnIndexes);
    }

    @Override
    public long getLockOwnerTid(long tid, int[] columnIndexes) {
        return tv.getLockOwnerTid(tid, columnIndexes);
    }

    @Override
    public String getHostAndPort() {
        return tv.getHostAndPort();
    }

    @Override
    public String getGlobalReplicationName() {
        return tv.getGlobalReplicationName();
    }

    @Override
    public boolean isReplicated() {
        return tv.isReplicated();
    }

    @Override
    public void setReplicated(boolean replicated) {
        tv.setReplicated(replicated);
    }

    @Override
    public List<String> getRetryReplicationNames() {
        return retryReplicationNames;
    }

    @Override
    public void setRetryReplicationNames(List<String> retryReplicationNames) {
        this.retryReplicationNames = retryReplicationNames;
    }

    @Override
    public void incrementVersion() {
        tv.incrementVersion();
    }

    @Override
    public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key) {
        return tv.undo(map, key);
    }

    @Override
    public TransactionalValue getCommitted() {
        return tv.getCommitted();
    }

    @Override
    public TransactionalValue getCommitted(AMTransaction transaction) {
        return tv.getCommitted(transaction);
    }

    @Override
    public TransactionalValue commit(long tid) {
        return tv.commit(tid);
    }

    @Override
    public void rollback() {
        tv.rollback();
    }

    @Override
    public TransactionalValue remove(long tid) {
        return tv.remove(tid);
    }

    @Override
    public boolean isCommitted() {
        return tv == null || tv.isCommitted();
    }

    @Override
    public void write(DataBuffer buff, StorageDataType valueType) {
        tv.write(buff, valueType);
    }

    @Override
    public void writeMeta(DataBuffer buff) {
        tv.writeMeta(buff);
    }

    @Override
    public void writeValue(DataBuffer buff, StorageDataType valueType) {
        tv.writeValue(buff, valueType);
    }
}
