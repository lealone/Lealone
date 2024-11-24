/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.log;

import com.lealone.db.DataBuffer;
import com.lealone.db.lock.Lockable;
import com.lealone.storage.StorageMap;
import com.lealone.storage.page.IPageReference;
import com.lealone.transaction.aote.AOTransactionEngine;
import com.lealone.transaction.aote.log.UndoLogRecord.KeyOnlyULR;
import com.lealone.transaction.aote.log.UndoLogRecord.KeyValueULR;

// 只有一个线程访问
public class UndoLog {

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

    public UndoLogRecord add(StorageMap<?, ?> map, Object key, Lockable lockable, Object oldValue) {
        if (map.getKeyType().isKeyOnly())
            return add(new KeyOnlyULR(map, key, lockable, oldValue));
        else
            return add(new KeyValueULR(map, key, lockable, oldValue));
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
        IPageReference last = null;
        UndoLogRecord r = first;
        while (r != null) {
            last = r.commit(te, last);
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

    public void toRedoLogRecordBuffer(DataBuffer buffer) {
        UndoLogRecord r = first;
        while (r != null) {
            r.writeForRedo(buffer);
            r = r.next;
        }
    }
}
