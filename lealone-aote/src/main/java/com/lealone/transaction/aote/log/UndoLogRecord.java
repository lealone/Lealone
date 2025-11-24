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
import com.lealone.storage.FormatVersion;
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
    public void commit(AOTransactionEngine te) {
        if (ignore())
            return;

        if (oldValue == null) { // insert
            TransactionalValue.commit(true, map, key, lockable);
        } else if (lockable.getLockedValue() == null) { // delete
            if (!te.containsRepeatableReadTransactions()) {
                lockable.getPageListener().getPageReference().remove(key);
            } else {
                map.decrementSize(); // 要减去1
                TransactionalValue.commit(false, map, key, lockable);
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
            lockable.getPageListener().getPageReference().remove(key);
        } else {
            TransactionalValue.rollback(oldValue, lockable);
        }
    }

    public abstract int writeForRedo(Map<StorageMap<Object, ?>, DataBuffer> logs,
            Map<String, StorageMap<?, ?>> maps);

    public static class KeyOnlyULR extends UndoLogRecord {

        public KeyOnlyULR(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
            super(map, key, lockable, oldValue);
        }

        @Override
        protected void commitUpdate() {
            // 没有update
        }

        @Override
        public int writeForRedo(Map<StorageMap<Object, ?>, DataBuffer> logs,
                Map<String, StorageMap<?, ?>> maps) {
            // 直接结束了
            // 比如索引不用写redo log
            return 0;
        }
    }

    public static class KeyValueULR extends UndoLogRecord {

        // 写redo log时需要用它，不能直接使用lockable.getValue()，因为会变动
        private final Object newValue;
        private final int metaVersion;

        public KeyValueULR(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
            super(map, key, lockable, oldValue);
            this.newValue = lockable.getLockedValue();
            this.metaVersion = lockable.getMetaVersion();
        }

        @Override
        protected void commitUpdate() {
            int memory = map.getValueType().getColumnsMemory(lockable.getLockedValue())
                    - map.getValueType().getColumnsMemory(oldValue);
            if (memory != 0)
                lockable.getPageListener().getPageReference().addPageUsedMemory(memory);

            TransactionalValue.commit(false, map, key, lockable);
        }

        @Override // 写redo log时，不关心oldValue
        public int writeForRedo(Map<StorageMap<Object, ?>, DataBuffer> logs,
                Map<String, StorageMap<?, ?>> maps) {
            if (ignore() || map.isInMemory() || !maps.containsKey(map.getName()))
                return 0;

            DataBuffer log = logs.get(map);
            if (log == null) {
                log = DataBuffer.createHeap();
                logs.put(map, log);
            }
            int pos = log.position();
            if (newValue == null) {
                log.put((byte) 0);
                map.getKeyType().write(log, key, FormatVersion.FORMAT_VERSION);
            } else {
                log.put((byte) 1);
                log.putVarInt(metaVersion);
                map.getKeyType().write(log, key, FormatVersion.FORMAT_VERSION);
                // 如果这里运行时出现了cast异常，可能是上层应用没有通过TransactionMap提供的api来写入最初的数据
                map.getValueType().getRawType().write(log, lockable, newValue,
                        FormatVersion.FORMAT_VERSION);
            }
            return log.position() - pos;
        }
    }

    // 兼容老版本的redo log
    public static void readForRedo(ByteBuffer buff, Map<String, List<ByteBuffer>> pendingRedoLog) {
        while (buff.hasRemaining()) {
            // 此时还没有打开底层存储的map，所以只预先解析出mapName和keyValue字节数组
            String mapName = ValueString.type.read(buff, FormatVersion.FORMAT_VERSION_1);
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
