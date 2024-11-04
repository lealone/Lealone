/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.log;

import com.lealone.db.DataBuffer;
import com.lealone.db.lock.Lockable;
import com.lealone.db.value.ValueString;
import com.lealone.storage.StorageMap;
import com.lealone.transaction.aote.AOTransactionEngine;
import com.lealone.transaction.aote.TransactionalValue;

public abstract class UndoLogRecord {

    protected final StorageMap<Object, ?> map;
    protected final Object key;
    protected final Lockable lockable;
    protected final Object oldValue;
    protected boolean undone;

    UndoLogRecord next;
    UndoLogRecord prev;

    @SuppressWarnings("unchecked")
    public UndoLogRecord(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
        this.map = (StorageMap<Object, ?>) map;
        this.key = key;
        this.lockable = lockable;
        this.oldValue = oldValue;
    }

    public void setUndone(boolean undone) {
        this.undone = undone;
    }

    protected boolean ignore() {
        // 事务取消或map关闭或删除时直接忽略
        return undone || map.isClosed();
    }

    protected abstract void commitUpdate();

    // 调用这个方法时事务已经提交，redo日志已经写完，这里只是在内存中更新到最新值
    // 不需要调用map.markDirty(key)，这很耗时，在下一步通过markDirtyPages()调用
    public void commit(AOTransactionEngine te) {
        if (ignore())
            return;

        if (oldValue == null) { // insert
            TransactionalValue.commit(true, map, lockable);
        } else if (lockable != null && lockable.getLockedValue() == null) { // delete
            if (!te.containsRepeatableReadTransactions()) {
                map.remove(key);
            } else {
                map.decrementSize(); // 要减去1
                TransactionalValue.commit(false, map, lockable);
            }
        } else { // update
            commitUpdate();
        }
    }

    // 当前事务开始rollback了，调用这个方法在内存中撤销之前的更新
    public void rollback(AOTransactionEngine te) {
        if (ignore())
            return;

        if (oldValue == null) {
            map.remove(key);
        } else {
            TransactionalValue.rollback(oldValue, lockable);
        }
    }

    protected abstract void writeForRedo0(DataBuffer writeBuffer);

    public void writeForRedo(DataBuffer writeBuffer) {
        if (ignore())
            return;

        if (oldValue != null) {
            map.markDirty(key);
        }
        writeForRedo0(writeBuffer);
    }

    public static class KeyOnlyULR extends UndoLogRecord {

        public KeyOnlyULR(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
            super(map, key, lockable, oldValue);
        }

        @Override
        protected void commitUpdate() {
            // 没有update
        }

        @Override
        protected void writeForRedo0(DataBuffer writeBuffer) {
            // 直接结束了
            // 比如索引不用写redo log
        }
    }

    public static class KeyValueULR extends UndoLogRecord {

        // 使用LazyRedoLogRecord时需要用它，不能直接使用lockable.getValue()，因为会变动
        private final Object newValue;

        public KeyValueULR(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
            super(map, key, lockable, oldValue);
            this.newValue = lockable.getLockedValue();
        }

        @Override
        protected void commitUpdate() {
            TransactionalValue.commit(false, map, lockable);
        }

        // 用于redo时，不关心oldValue
        @Override
        protected void writeForRedo0(DataBuffer writeBuffer) {
            ValueString.type.write(writeBuffer, map.getName());
            int keyValueLengthStartPos = writeBuffer.position();
            writeBuffer.putInt(0);

            map.getKeyType().write(writeBuffer, key);
            if (newValue == null)
                writeBuffer.put((byte) 0);
            else {
                writeBuffer.put((byte) 1);
                // 如果这里运行时出现了cast异常，可能是上层应用没有通过TransactionMap提供的api来写入最初的数据
                map.getValueType().getRawType().write(writeBuffer, newValue, lockable);
            }
            writeBuffer.putInt(keyValueLengthStartPos,
                    writeBuffer.position() - keyValueLengthStartPos - 4);
        }
    }
}
