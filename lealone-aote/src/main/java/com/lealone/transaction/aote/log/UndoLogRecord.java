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
    protected boolean undone;

    UndoLogRecord next;
    UndoLogRecord prev;

    @SuppressWarnings("unchecked")
    public UndoLogRecord(StorageMap<?, ?> map, Object key, Lockable lockable) {
        this.map = (StorageMap<Object, ?>) map;
        this.key = key;
        this.lockable = lockable;
    }

    public void setUndone(boolean undone) {
        this.undone = undone;
    }

    protected boolean ignore() {
        // 事务取消或map关闭或删除时直接忽略
        return undone || map.isClosed();
    }

    public abstract void commit(AOTransactionEngine te);

    public abstract void rollback(AOTransactionEngine te);

    public abstract void writeForRedo(DataBuffer writeBuffer, AOTransactionEngine te);

    public static class KeyOnlyULR extends UndoLogRecord {

        private final boolean isInsert;

        public KeyOnlyULR(StorageMap<?, ?> map, Object key, Lockable lockable, boolean isInsert) {
            super(map, key, lockable);
            this.isInsert = isInsert;
        }

        @Override
        public void commit(AOTransactionEngine te) {
            if (ignore())
                return;

            if (isInsert) { // insert
                TransactionalValue.commit(true, map, lockable);
            } else if (lockable != null && lockable.getLockedValue() == null) { // delete
                if (!te.containsRepeatableReadTransactions()) {
                    map.remove(key);
                } else {
                    map.decrementSize(); // 要减去1
                    TransactionalValue.commit(false, map, lockable);
                }
            }
            // 没有update
        }

        @Override
        public void rollback(AOTransactionEngine te) {
            if (ignore())
                return;

            if (isInsert) {
                map.remove(key);
            } else {
                TransactionalValue.rollback(null, lockable);
            }
        }

        @Override
        public void writeForRedo(DataBuffer writeBuffer, AOTransactionEngine te) {
            if (ignore())
                return;

            if (!isInsert) {
                map.markDirty(key);
            }
            // 直接结束了
            // 比如索引不用写redo log
        }
    }

    public static class KeyValueULR extends UndoLogRecord {

        // 使用LazyRedoLogRecord时需要用它，不能直接使用lockable.getValue()，因为会变动
        private final Object newValue;
        private final Object oldValue;

        public KeyValueULR(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
            super(map, key, lockable);
            this.newValue = lockable.getLockedValue();
            this.oldValue = oldValue;
        }

        // 调用这个方法时事务已经提交，redo日志已经写完，这里只是在内存中更新到最新值
        // 不需要调用map.markDirty(key)，这很耗时，在下一步通过markDirtyPages()调用
        @Override
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
                TransactionalValue.commit(false, map, lockable);
            }
        }

        // 当前事务开始rollback了，调用这个方法在内存中撤销之前的更新
        @Override
        public void rollback(AOTransactionEngine te) {
            if (ignore())
                return;

            if (oldValue == null) {
                map.remove(key);
            } else {
                TransactionalValue.rollback(oldValue, lockable);
            }
        }

        // 用于redo时，不关心oldValue
        @Override
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
                map.getValueType().getRawType().write(writeBuffer, newValue, lockable);
            }
            writeBuffer.putInt(keyValueLengthStartPos,
                    writeBuffer.position() - keyValueLengthStartPos - 4);
        }
    }
}
