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

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.link.LinkableBase;
import com.lealone.db.value.ValueString;

public abstract class RedoLogRecord {

    private static byte TYPE_CHECKPOINT = 0;
    private static byte TYPE_DROPPED_MAP = 1;
    private static byte TYPE_LOCAL_TRANSACTION = 2;

    public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
    }

    boolean isCheckpoint() {
        return false;
    }

    abstract void write(DataBuffer buff);

    static RedoLogRecord read(ByteBuffer buff) {
        int type = buff.get();
        if (type == TYPE_CHECKPOINT) {
            return CheckpointRLR.read(buff);
        } else if (type == TYPE_DROPPED_MAP) {
            return DroppedMapRLR.read(buff);
        } else if (type == TYPE_LOCAL_TRANSACTION) {
            return LocalTransactionRLR.read(buff);
        } else {
            throw DbException.getInternalError("unknow type: " + type);
        }
    }

    public static CheckpointRLR createCheckpoint(long checkpointId, boolean saved) {
        return new CheckpointRLR(checkpointId, saved);
    }

    public static DroppedMapRLR createDroppedMapRedoLogRecord(String mapName) {
        return new DroppedMapRLR(mapName);
    }

    public static PendingCheckpoint createPendingCheckpoint(long checkpointId, boolean saved,
            boolean force) {
        return new PendingCheckpoint(new CheckpointRLR(checkpointId, saved), force);
    }

    public static class PendingCheckpoint extends LinkableBase<PendingCheckpoint> {

        private final CheckpointRLR checkpoint;
        private boolean synced;
        private boolean force;

        public PendingCheckpoint(CheckpointRLR checkpoint, boolean force) {
            this.checkpoint = checkpoint;
        }

        public CheckpointRLR getCheckpoint() {
            return checkpoint;
        }

        public long getCheckpointId() {
            return checkpoint.getCheckpointId();
        }

        public boolean isSaved() {
            return checkpoint.isSaved();
        }

        public boolean isForce() {
            return force;
        }

        public boolean isSynced() {
            return synced;
        }

        public void setSynced(boolean synced) {
            this.synced = synced;
        }
    }

    static class CheckpointRLR extends RedoLogRecord {

        private final long checkpointId;
        private final boolean saved;

        CheckpointRLR(long checkpointId, boolean saved) {
            this.checkpointId = checkpointId;
            this.saved = saved;
        }

        public long getCheckpointId() {
            return checkpointId;
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
            return new CheckpointRLR(0, true);
        }
    }

    static class DroppedMapRLR extends RedoLogRecord {

        private final String mapName;

        DroppedMapRLR(String mapName) {
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
            return new DroppedMapRLR(mapName);
        }
    }

    static class TransactionRLR extends RedoLogRecord {

        protected ByteBuffer operations;

        public TransactionRLR(ByteBuffer operations) {
            this.operations = operations;
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
            write(buff, TYPE_LOCAL_TRANSACTION);
        }

        public void write(DataBuffer buff, byte type) {
            buff.put(type);
            buff.putVarLong(0); // transactionId兼容老版本
            writeOperations(buff);
        }

        public void writeOperations(DataBuffer buff) {
            buff.putInt(operations.remaining());
            buff.put(operations);
        }

        public static ByteBuffer readOperations(ByteBuffer buff) {
            byte[] bytes = new byte[buff.getInt()];
            buff.get(bytes);
            return ByteBuffer.wrap(bytes);
        }
    }

    public static class LocalTransactionRLR extends TransactionRLR {

        public LocalTransactionRLR(ByteBuffer operations) {
            super(operations);
        }

        public static LocalTransactionRLR read(ByteBuffer buff) {
            DataUtils.readVarLong(buff); // transactionId兼容老版本
            ByteBuffer operations = readOperations(buff);
            return new LocalTransactionRLR(operations);
        }
    }

    public static class LazyLocalTransactionRLR extends LocalTransactionRLR {

        private final UndoLog undoLog;

        public LazyLocalTransactionRLR(UndoLog undoLog) {
            super((ByteBuffer) null);
            this.undoLog = undoLog;
        }

        @Override
        public void writeOperations(DataBuffer buff) {
            writeOperations(buff, undoLog);
        }

        static void writeOperations(DataBuffer buff, UndoLog undoLog) {
            int pos = buff.position();
            buff.putInt(0);
            undoLog.toRedoLogRecordBuffer(buff);
            buff.putInt(pos, buff.position() - pos - 4);
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
        public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
        }

        @Override
        void write(DataBuffer buff) {
            lobTask.run();
            r.write(buff);
        }
    }
}
