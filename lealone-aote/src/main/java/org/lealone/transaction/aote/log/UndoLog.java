/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import org.lealone.db.DataBuffer;
import org.lealone.transaction.aote.AOTransactionEngine;
import org.lealone.transaction.aote.TransactionalValue;

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

    public UndoLogRecord add(String mapName, Object key, Object oldValue, TransactionalValue newTV,
            boolean isForUpdate) {
        UndoLogRecord r = new UndoLogRecord(mapName, key, oldValue, newTV, isForUpdate);
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

    public void undo() {
        removeLast();
    }

    public void commit(AOTransactionEngine transactionEngine) {
        UndoLogRecord r = first;
        while (r != null) {
            r.commit(transactionEngine);
            r = r.next;
        }
    }

    public void rollbackTo(AOTransactionEngine transactionEngine, int toLogId) {
        while (logId > toLogId) {
            UndoLogRecord r = removeLast();
            r.rollback(transactionEngine);
        }
    }

    // 将当前一系列的事务操作日志转换成单条RedoLogRecord
    public DataBuffer toRedoLogRecordBuffer(AOTransactionEngine transactionEngine) {
        if (isEmpty())
            return null;
        DataBuffer buffer = DataBuffer.create();
        UndoLogRecord r = first;
        while (r != null) {
            r.writeForRedo(buffer, transactionEngine);
            r = r.next;
        }
        buffer.getAndFlipBuffer();
        return buffer;
    }
}
