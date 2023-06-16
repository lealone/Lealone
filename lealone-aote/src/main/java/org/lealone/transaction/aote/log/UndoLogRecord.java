/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.aote.AOTransactionEngine;
import org.lealone.transaction.aote.TransactionalValue;
import org.lealone.transaction.aote.TransactionalValueType;

public class UndoLogRecord {

    private final String mapName;
    private final Object key;
    private final Object oldValue;
    private final TransactionalValue newTV;
    private boolean undone;

    UndoLogRecord next;
    UndoLogRecord prev;

    public UndoLogRecord(String mapName, Object key, Object oldValue, TransactionalValue newTV) {
        this.mapName = mapName;
        this.key = key;
        this.oldValue = oldValue;
        this.newTV = newTV;
    }

    public String getMapName() {
        return mapName;
    }

    public Object getKey() {
        return key;
    }

    public UndoLogRecord getNext() {
        return next;
    }

    public void setUndone(boolean undone) {
        this.undone = undone;
    }

    // 调用这个方法时事务已经提交，redo日志已经写完，这里只是在内存中更新到最新值
    public void commit(AOTransactionEngine te) {
        if (undone)
            return;
        StorageMap<Object, TransactionalValue> map = te.getStorageMap(mapName);
        if (map == null) {
            return; // map was later removed
        }
        if (oldValue == null) { // insert
            newTV.commit(true);
        } else if (newTV != null && newTV.getValue() == null) { // delete
            if (!te.containsRepeatableReadTransactions()) {
                newTV.setValue(oldValue); // 用于计算内存
                map.remove(key);
            } else {
                map.decrementSize(); // 要减去1
                newTV.commit(false);
                map.markDirty(key);
            }
        } else { // update
            newTV.commit(false);
            map.markDirty(key); // 无需put回去，标记一下脏页即可
        }
    }

    // 当前事务开始rollback了，调用这个方法在内存中撤销之前的更新
    public void rollback(AOTransactionEngine te) {
        if (undone)
            return;
        StorageMap<Object, TransactionalValue> map = te.getStorageMap(mapName);
        // 有可能在执行DROP DATABASE时删除了
        if (map != null) {
            if (oldValue == null) {
                map.remove(key);
            } else {
                newTV.rollback(oldValue);
            }
        }
    }

    // 用于redo时，不关心oldValue
    public void writeForRedo(DataBuffer writeBuffer, AOTransactionEngine te) {
        if (undone)
            return;
        StorageMap<?, ?> map = te.getStorageMap(mapName);
        // 有可能在执行DROP DATABASE时删除了
        if (map == null) {
            return;
        }

        ValueString.type.write(writeBuffer, mapName);
        int keyValueLengthStartPos = writeBuffer.position();
        writeBuffer.putInt(0);

        map.getKeyType().write(writeBuffer, key);
        if (newTV.getValue() == null)
            writeBuffer.put((byte) 0);
        else {
            writeBuffer.put((byte) 1);
            // 如果这里运行时出现了cast异常，可能是上层应用没有通过TransactionMap提供的api来写入最初的数据
            ((TransactionalValueType) map.getValueType()).valueType.write(writeBuffer, newTV.getValue());
        }
        writeBuffer.putInt(keyValueLengthStartPos, writeBuffer.position() - keyValueLengthStartPos - 4);
    }
}
