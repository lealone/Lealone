/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.transaction.aote.AOTransaction;

public abstract class RedoLogRecord {

    private static byte TYPE_CHECKPOINT = 0;
    private static byte TYPE_DROPPED_MAP = 1;
    private static byte TYPE_TRANSACTION_REDO_LOG_RECORD = 2;

    private AOTransaction transaction;
    private CountDownLatch latch;

    public void setTransaction(AOTransaction transaction) {
        this.transaction = transaction;
    }

    void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    void onSynced() {
        if (latch != null) {
            latch.countDown();
        } else if (transaction != null) {
            transaction.asyncCommitComplete();
            transaction = null;
        }
    }

    boolean isCheckpoint() {
        return false;
    }

    abstract void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog);

    abstract void write(DataBuffer buff);

    static RedoLogRecord read(ByteBuffer buff) {
        int type = buff.get();
        if (type == TYPE_CHECKPOINT) {
            return Checkpoint.read(buff);
        } else if (type == TYPE_DROPPED_MAP) {
            return DroppedMapRedoLogRecord.read(buff);
        } else if (type == TYPE_TRANSACTION_REDO_LOG_RECORD) {
            return TransactionRedoLogRecord.read(buff);
        } else {
            throw DbException.getInternalError("unknow type: " + type);
        }
    }

    public static Checkpoint createCheckpoint(boolean saved) {
        return new Checkpoint(saved);
    }

    public static DroppedMapRedoLogRecord createDroppedMapRedoLogRecord(String mapName) {
        return new DroppedMapRedoLogRecord(mapName);
    }

    public static TransactionRedoLogRecord createTransactionRedoLogRecord(DataBuffer operations) {
        return new TransactionRedoLogRecord(operations);
    }

    static class Checkpoint extends RedoLogRecord {

        private final boolean saved;

        Checkpoint(boolean saved) {
            this.saved = saved;
        }

        @Override
        public boolean isCheckpoint() {
            return true;
        }

        public boolean isSaved() {
            return saved;
        }

        @Override
        public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
            pendingRedoLog.clear();
        }

        @Override
        public void write(DataBuffer buff) {
            buff.put(TYPE_CHECKPOINT);
            buff.putVarLong(0); // checkpointId兼容老版本
        }

        public static RedoLogRecord read(ByteBuffer buff) {
            DataUtils.readVarLong(buff); // checkpointId兼容老版本
            return new Checkpoint(true);
        }
    }

    static class DroppedMapRedoLogRecord extends RedoLogRecord {

        private final String mapName;

        DroppedMapRedoLogRecord(String mapName) {
            DataUtils.checkNotNull(mapName, "mapName");
            this.mapName = mapName;
        }

        @Override
        public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
            List<ByteBuffer> logs = pendingRedoLog.get(mapName);
            if (logs != null) {
                logs = new LinkedList<>();
                pendingRedoLog.put(mapName, logs);
            }
        }

        @Override
        public void write(DataBuffer buff) {
            buff.put(TYPE_DROPPED_MAP);
            ValueString.type.write(buff, mapName);
        }

        public static RedoLogRecord read(ByteBuffer buff) {
            String mapName = ValueString.type.read(buff);
            return new DroppedMapRedoLogRecord(mapName);
        }
    }

    static class TransactionRedoLogRecord extends RedoLogRecord {

        private final ByteBuffer operations;
        private DataBuffer buffer;

        public TransactionRedoLogRecord(ByteBuffer operations) {
            this.operations = operations;
        }

        public TransactionRedoLogRecord(DataBuffer operations) {
            this(operations.getBuffer());
            this.buffer = operations;
        }

        @Override
        public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
            ByteBuffer buff = operations;
            while (buff.hasRemaining()) {
                // 此时还没有打开底层存储的map，所以只预先解析出mapName和keyValue字节数组
                // 写时格式参照UndoLogRecord.writeForRedo()
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

        @Override
        public void write(DataBuffer buff) {
            buff.put(TYPE_TRANSACTION_REDO_LOG_RECORD);
            buff.putVarLong(0); // transactionId兼容老版本
            buff.putInt(operations.remaining());
            buff.put(operations);
            if (buffer != null) {
                buffer.close();
            }
        }

        public static TransactionRedoLogRecord read(ByteBuffer buff) {
            DataUtils.readVarLong(buff); // transactionId兼容老版本
            byte[] bytes = new byte[buff.getInt()];
            buff.get(bytes);
            return new TransactionRedoLogRecord(ByteBuffer.wrap(bytes));
        }
    }

    public static class LobSave extends RedoLogRecord {

        Runnable lobTask;
        RedoLogRecord r;

        public LobSave(Runnable lobTask, RedoLogRecord r) {
            this.lobTask = lobTask;
            this.r = r;
        }

        @Override
        void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
        }

        @Override
        void write(DataBuffer buff) {
            lobTask.run();
            r.write(buff);
        }
    }
}
