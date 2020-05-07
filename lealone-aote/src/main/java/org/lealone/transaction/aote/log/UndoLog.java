/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.transaction.aote.log;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.lealone.db.DataBuffer;
import org.lealone.transaction.aote.AMTransactionEngine;
import org.lealone.transaction.aote.TransactionalValue;

public class UndoLog {

    private int logId;
    private final LinkedList<UndoLogRecord> undoLogRecords = new LinkedList<>();

    public int getLogId() {
        return logId;
    }

    public LinkedList<UndoLogRecord> getUndoLogRecords() {
        return undoLogRecords;
    }

    public boolean isEmpty() {
        return undoLogRecords.isEmpty();
    }

    public boolean isNotEmpty() {
        return !undoLogRecords.isEmpty();
    }

    public UndoLogRecord getLast() {
        return undoLogRecords.getLast();
    }

    public int size() {
        return undoLogRecords.size();
    }

    public UndoLogRecord add(String mapName, Object key, TransactionalValue oldValue, TransactionalValue newValue,
            boolean isForUpdate) {
        UndoLogRecord r = new UndoLogRecord(mapName, key, oldValue, newValue, isForUpdate);
        undoLogRecords.add(r);
        logId++;
        return r;
    }

    public UndoLogRecord add(String mapName, Object key, TransactionalValue oldValue, TransactionalValue newValue) {
        return add(mapName, key, oldValue, newValue, false);
    }

    public void undo() {
        undoLogRecords.removeLast();
        --logId;
    }

    public void commit(AMTransactionEngine transactionEngine, long tid) {
        for (UndoLogRecord r : undoLogRecords) {
            r.commit(transactionEngine, tid);
        }
    }

    public void rollbackTo(AMTransactionEngine transactionEngine, long toLogId) {
        while (logId > toLogId) {
            UndoLogRecord r = undoLogRecords.removeLast();
            r.rollback(transactionEngine);
            --logId;
        }
    }

    private static int lastCapacity = 1024;

    // 将当前一系列的事务操作日志转换成单条RedoLogRecord
    public ByteBuffer toRedoLogRecordBuffer(AMTransactionEngine transactionEngine) {
        if (undoLogRecords.isEmpty())
            return null;
        DataBuffer writeBuffer = DataBuffer.create(lastCapacity);
        for (UndoLogRecord r : undoLogRecords) {
            r.writeForRedo(writeBuffer, transactionEngine);
        }
        lastCapacity = writeBuffer.position();
        if (lastCapacity > 1024)
            lastCapacity = 1024;
        return writeBuffer.getAndFlipBuffer();
    }
}
