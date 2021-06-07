/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.List;

import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.aote.AMTransactionEngine;
import org.lealone.transaction.aote.TransactionalValue;
import org.lealone.transaction.aote.TransactionalValueType;

public class UndoLogRecord {

    private final String mapName;
    private Object key; // 没有用final，在AMTransaction.replicationPrepareCommit方法那里有特殊用途
    private final TransactionalValue oldValue;
    private final TransactionalValue newValue;
    private final boolean isForUpdate;
    private volatile boolean undone;

    public UndoLogRecord(String mapName, Object key, TransactionalValue oldValue, TransactionalValue newValue,
            boolean isForUpdate) {
        this.mapName = mapName;
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.isForUpdate = isForUpdate;
    }

    public String getMapName() {
        return mapName;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public void setUndone(boolean undone) {
        this.undone = undone;
    }

    public void setRetryReplicationNames(List<String> retryReplicationNames) {
        if (oldValue != null && oldValue.getRef() != null)
            oldValue.getRef().setRetryReplicationNames(retryReplicationNames);
    }

    // 调用这个方法时事务已经提交，redo日志已经写完，这里只是在内存中更新到最新值
    public void commit(AMTransactionEngine transactionEngine, long tid) {
        if (undone)
            return;
        if (isForUpdate) {
            newValue.rollback(); // 解锁而已，不用提交的
            return;
        }
        StorageMap<Object, TransactionalValue> map = transactionEngine.getStorageMap(mapName);
        if (map == null) {
            return; // map was later removed
        }
        if (oldValue == null) { // insert
            newValue.commit(tid);
        } else if (newValue != null && newValue.getValue() == null) { // delete
            if (!transactionEngine.containsRepeatableReadTransactions(tid)) {
                map.remove(key);
            } else {
                newValue.commit(tid);
            }
            // newValue.commit(tid);
        } else { // update
            TransactionalValue ref = newValue.commit(tid);
            // 先删除后增加的场景，需要重新put回去
            if (newValue.getOldValue() != null && newValue.getOldValue().getValue() == null) {
                map.put(key, ref);
            }
        }
    }

    // 当前事务开始rollback了，调用这个方法在内存中撤销之前的更新
    public void rollback(AMTransactionEngine transactionEngine) {
        if (undone)
            return;
        if (isForUpdate) {
            newValue.rollback();
            return;
        }
        StorageMap<Object, TransactionalValue> map = transactionEngine.getStorageMap(mapName);
        // 有可能在执行DROP DATABASE时删除了
        if (map != null) {
            if (oldValue == null) {
                map.remove(key);
            } else {
                newValue.rollback();
            }
        }
    }

    // 用于redo时，不关心oldValue
    public void writeForRedo(DataBuffer writeBuffer, AMTransactionEngine transactionEngine) {
        if (isForUpdate || undone) {
            return;
        }
        StorageMap<?, ?> map = transactionEngine.getStorageMap(mapName);
        // 有可能在执行DROP DATABASE时删除了
        if (map == null) {
            return;
        }
        int lastPosition = writeBuffer.position();

        ValueString.type.write(writeBuffer, mapName);
        int keyValueLengthStartPos = writeBuffer.position();
        writeBuffer.putInt(0);

        map.getKeyType().write(writeBuffer, key);
        if (newValue.getValue() == null)
            writeBuffer.put((byte) 0);
        else {
            writeBuffer.put((byte) 1);
            // 如果这里运行时出现了cast异常，可能是上层应用没有通过TransactionMap提供的api来写入最初的数据
            ((TransactionalValueType) map.getValueType()).valueType.write(writeBuffer, newValue.getValue());
        }
        writeBuffer.putInt(keyValueLengthStartPos, writeBuffer.position() - keyValueLengthStartPos - 4);

        // 预估一下内存占用大小，当到达一个阈值时方便其他服务线程刷数据到硬盘
        int memory = writeBuffer.position() - lastPosition;
        transactionEngine.incrementEstimatedMemory(mapName, memory);
    }
}
