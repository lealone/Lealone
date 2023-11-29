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

    private final StorageMap<Object, ?> map;
    private final Object key;
    private final Object oldValue;
    private final Object newValue; // 使用LazyRedoLogRecord时需要用它，不能直接使用newTV.getValue()，因为会变动
    private final TransactionalValue newTV;
    private boolean undone;

    UndoLogRecord next;
    UndoLogRecord prev;

    @SuppressWarnings("unchecked")
    public UndoLogRecord(StorageMap<?, ?> map, Object key, Object oldValue, TransactionalValue newTV) {
        this.map = (StorageMap<Object, ?>) map;
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newTV.getValue();
        this.newTV = newTV;
    }

    public void setUndone(boolean undone) {
        this.undone = undone;
    }

    private boolean ignore() {
        // 事务取消或map关闭或删除时直接忽略
        return undone || map.isClosed();
    }

    // 调用这个方法时事务已经提交，redo日志已经写完，这里只是在内存中更新到最新值
    // 不需要调用map.markDirty(key)，这很耗时，在下一步通过markDirtyPages()调用
    public void commit(AOTransactionEngine te) {
        if (ignore())
            return;

        if (oldValue == null) { // insert
            newTV.commit(true, map);
        } else if (newTV != null && newTV.getValue() == null) { // delete
            if (!te.containsRepeatableReadTransactions()) {
                map.remove(key);
            } else {
                map.decrementSize(); // 要减去1
                newTV.commit(false, map);
            }
        } else { // update
            newTV.commit(false, map);
        }
    }

    // 当前事务开始rollback了，调用这个方法在内存中撤销之前的更新
    public void rollback(AOTransactionEngine te) {
        if (ignore())
            return;

        if (oldValue == null) {
            map.remove(key);
        } else {
            newTV.rollback(oldValue);
        }
    }

    // 用于redo时，不关心oldValue
    public void writeForRedo(DataBuffer writeBuffer, AOTransactionEngine te) {
        if (ignore())
            return;

        // 这一步很重要！！！
        // 对于update和delete，要标记一下脏页，否则执行checkpoint保存数据时无法实别脏页
        if (oldValue != null) {
            map.markDirty(key);
        }

        ValueString.type.write(writeBuffer, map.getName());
        int keyValueLengthStartPos = writeBuffer.position();
        writeBuffer.putInt(0);

        map.getKeyType().write(writeBuffer, key);
        if (newValue == null)
            writeBuffer.put((byte) 0);
        else {
            writeBuffer.put((byte) 1);
            // 如果这里运行时出现了cast异常，可能是上层应用没有通过TransactionMap提供的api来写入最初的数据
            ((TransactionalValueType) map.getValueType()).valueType.write(writeBuffer, newValue);
        }
        writeBuffer.putInt(keyValueLengthStartPos, writeBuffer.position() - keyValueLengthStartPos - 4);
    }
}
