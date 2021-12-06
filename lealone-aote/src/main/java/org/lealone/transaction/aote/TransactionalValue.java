/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;
import java.util.List;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

public interface TransactionalValue {

    // 对于一个已经提交的值，如果当前事务因为隔离级别的原因读不到这个值，那么就返回SIGHTLESS
    public static final TransactionalValue SIGHTLESS = createCommitted(null);

    public Object getValue();

    public Object getValue(AMTransaction transaction);

    public void setValue(Object value);

    public TransactionalValue getOldValue();

    public void setOldValue(TransactionalValue oldValue);

    // 如果是0代表事务已经提交，对于已提交事务，只有在写入时才写入tid=0，
    // 读出来的时候为了不占用内存就不加tid字段了，这样每条已提交记录能省8个字节(long)的内存空间
    public long getTid();

    public int getLogId();

    public boolean supportsColumnLock();

    public boolean tryLock(AMTransaction t, int[] columnIndexes);

    public void unlock();

    public boolean isLocked(long tid, int[] columnIndexes);

    public AMTransaction getLockOwner(int[] columnIndexes);

    public String getHostAndPort();

    public String getGlobalReplicationName();

    public boolean isReplicated();

    public void setReplicated(boolean replicated);

    public default List<String> getRetryReplicationNames() {
        return null;
    }

    public default void setRetryReplicationNames(List<String> retryReplicationNames) {
    }

    public void incrementVersion();

    public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key);

    public TransactionalValue getCommitted(AMTransaction transaction);

    public TransactionalValue commit(long tid);

    public void rollback();

    public boolean isCommitted();

    public default void gc(AMTransaction transaction) {
    }

    public void write(DataBuffer buff, StorageDataType valueType);

    public void writeMeta(DataBuffer buff);

    public void writeValue(DataBuffer buff, StorageDataType valueType);

    public static TransactionalValue readMeta(ByteBuffer buff, StorageDataType valueType, StorageDataType oldValueType,
            int columnCount) {
        long tid = DataUtils.readVarLong(buff);
        if (tid == 0) {
            Object value = valueType.readMeta(buff, columnCount);
            return TransactionalValue.createCommitted(value);
        } else {
            // return Uncommitted.readMeta(tid, valueType, buff, oldValueType, columnCount);
            return null;
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
            // return createRef(Uncommitted.read(tid, valueType, buff, oldValueType));

            return null;
        }
    }

    public static TransactionalValue createCommitted(Object value) {
        return new TValue(value);
    }
}
