/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.log;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.lealone.db.DataBuffer;
import com.lealone.db.lock.Lockable;
import com.lealone.storage.StorageMap;
import com.lealone.transaction.aote.AOTransactionEngine;
import com.lealone.transaction.aote.log.UndoLogRecord.KeyOnlyULR;
import com.lealone.transaction.aote.log.UndoLogRecord.KeyValueULR;

// 只有一个线程访问
public class UndoLog {

    // 保存需要写redo log的StorageMap，索引或内存表对应的StorageMap不需要写redo log
    private final ConcurrentHashMap<StorageMap<?, ?>, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();

    private int logId;
    private UndoLogRecord first;// 指向最早加进来的，执行commit时从first开始遍历
    private UndoLogRecord last; // 总是指向新增加的，执行rollback时从first开始遍历

    public int getLogId() {
        return logId;
    }

    public UndoLogRecord getFirst() {
        return first;
    }

    public int size() {
        return logId;
    }

    public boolean isEmpty() {
        return logId == 0;
    }

    public boolean isNotEmpty() {
        return logId != 0;
    }

    public ConcurrentHashMap<StorageMap<?, ?>, StorageMap<?, ?>> getMaps() {
        return maps;
    }

    public UndoLogRecord add(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
        if (map.getKeyType().isKeyOnly()) {
            return add(new KeyOnlyULR(map, key, lockable, oldValue));
        } else {
            if (!map.isInMemory())
                maps.put(map, map);
            return add(new KeyValueULR(map, key, lockable, oldValue));
        }
    }

    private UndoLogRecord add(UndoLogRecord r) {
        if (first == null) {
            first = last = r;
        } else {
            last.next = r;
            r.prev = last;
            last = r;
        }
        logId++;
        return r;
    }

    private UndoLogRecord removeLast() {
        UndoLogRecord r = last;
        if (last != null) {
            if (last.prev != null)
                last.prev.next = null;
            last = last.prev;
            if (last == null) {
                first = null;
            }
            --logId;
        }
        return r;
    }

    public int commit(AOTransactionEngine te) {
        UndoLogRecord r = first;
        while (r != null) {
            r.commit(te);
            r = r.next;
        }
        return logId;
    }

    public void rollbackTo(AOTransactionEngine te, int toLogId) {
        while (logId > toLogId) {
            UndoLogRecord r = removeLast();
            r.rollback(te);
        }
    }

    public int writeForRedo(Map<StorageMap<Object, ?>, DataBuffer> logs,
            Map<String, StorageMap<?, ?>> maps) {
        int len = 0;
        UndoLogRecord r = first;
        while (r != null) {
            if (r.map.isClosed())
                this.maps.remove(r.map);
            len += r.writeForRedo(logs, maps);
            r = r.next;
        }
        return len;
    }
}
