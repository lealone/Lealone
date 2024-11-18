/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.log;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.lealone.db.DataBuffer;
import com.lealone.db.lock.Lockable;
import com.lealone.db.value.ValueString;
import com.lealone.storage.StorageMap;
import com.lealone.storage.page.IPageReference;
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
    public IPageReference commit(AOTransactionEngine te, IPageReference last) {
        if (ignore())
            return null;

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
        // 标记脏页
        // IPageReference ref = lockable.getPageListener().getPageReference();
        // if (ref != last) // 避免反复标记
        // ref.markDirtyPage(lockable.getPageListener());
        // return ref;
        return null;
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

    public abstract void writeForRedo(DataBuffer buff);

    public static class KeyOnlyULR extends UndoLogRecord {

        public KeyOnlyULR(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
            super(map, key, lockable, oldValue);
        }

        @Override
        protected void commitUpdate() {
            // 没有update
        }

        @Override
        public void writeForRedo(DataBuffer buff) {
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

        @Override
        public void writeForRedo(DataBuffer buff) { // 写redo时，不关心oldValue
            if (ignore())
                return;

            ValueString.type.write(buff, map.getName());
            int keyValueLengthStartPos = buff.position();
            buff.putInt(0);

            map.getKeyType().write(buff, key);
            if (newValue == null)
                buff.put((byte) 0);
            else {
                buff.put((byte) 1);
                // 如果这里运行时出现了cast异常，可能是上层应用没有通过TransactionMap提供的api来写入最初的数据
                map.getValueType().getRawType().write(buff, newValue, lockable);
            }
            buff.putInt(keyValueLengthStartPos, buff.position() - keyValueLengthStartPos - 4);
        }
    }

    public static void readForRedo(ByteBuffer buff, Map<String, List<ByteBuffer>> pendingRedoLog) {
        while (buff.hasRemaining()) {
            // 此时还没有打开底层存储的map，所以只预先解析出mapName和keyValue字节数组
            String mapName = ValueString.type.read(buff);
            List<ByteBuffer> keyValues = pendingRedoLog.get(mapName);
            if (keyValues == null) {
                keyValues = new LinkedList<>();
                pendingRedoLog.put(mapName, keyValues);
            }
            int len = buff.getInt();
            byte[] keyValue = new byte[len];
            buff.get(keyValue);
            keyValues.add(ByteBuffer.wrap(keyValue));
        }
    }
}
