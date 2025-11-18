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
import java.util.concurrent.ConcurrentHashMap;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.value.ValueString;
import com.lealone.storage.StorageMap;

// initPendingRedoLog和read方法都是为了兼容老版本的redo log
// 新版本的redo log只需要write方法
public abstract class RedoLogRecord {

    public static RedoLogRecord read(ByteBuffer buff) {
        int type = buff.get();
        if (type == 0) {
            return CheckpointRLR.read(buff);
        } else if (type == 1) {
            return DroppedMapRLR.read(buff);
        } else if (type == 2) {
            return LocalTransactionRLR.read(buff);
        } else {
            throw DbException.getInternalError("unknow type: " + type);
        }
    }

    public boolean isCheckpoint() {
        return false;
    }

    public abstract void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog);

    public int write(Map<StorageMap<Object, ?>, DataBuffer> logs, Map<String, StorageMap<?, ?>> maps) {
        return 0;
    }

    public ConcurrentHashMap<StorageMap<?, ?>, StorageMap<?, ?>> getMaps() {
        return null;
    }

    public static class CheckpointRLR extends RedoLogRecord {

        @Override
        public boolean isCheckpoint() {
            return true;
        }

        @Override
        public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
            pendingRedoLog.clear();
        }

        public static RedoLogRecord read(ByteBuffer buff) {
            DataUtils.readVarLong(buff); // checkpointId兼容老版本
            return new CheckpointRLR();
        }
    }

    public static class DroppedMapRLR extends RedoLogRecord {

        private final String mapName;

        public DroppedMapRLR(String mapName) {
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

        public static RedoLogRecord read(ByteBuffer buff) {
            String mapName = ValueString.type.read(buff);
            return new DroppedMapRLR(mapName);
        }
    }

    public static class LocalTransactionRLR extends RedoLogRecord {

        private final UndoLog undoLog;
        private final ByteBuffer operations;

        public LocalTransactionRLR(UndoLog undoLog, ByteBuffer operations) {
            this.undoLog = undoLog;
            this.operations = operations;
        }

        @Override
        public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
            UndoLogRecord.readForRedo(operations, pendingRedoLog);
        }

        @Override
        public ConcurrentHashMap<StorageMap<?, ?>, StorageMap<?, ?>> getMaps() {
            return undoLog.getMaps();
        }

        @Override
        public int write(Map<StorageMap<Object, ?>, DataBuffer> logs,
                Map<String, StorageMap<?, ?>> maps) {
            return undoLog.writeForRedo(logs, maps);
        }

        public static LocalTransactionRLR read(ByteBuffer buff) {
            DataUtils.readVarLong(buff); // transactionId兼容老版本
            byte[] bytes = new byte[buff.getInt()];
            buff.get(bytes);
            ByteBuffer operations = ByteBuffer.wrap(bytes);
            return new LocalTransactionRLR(null, operations);
        }
    }

    public static class LobSave extends RedoLogRecord {

        private final Runnable lobTask;
        private final RedoLogRecord r;

        public LobSave(Runnable lobTask, RedoLogRecord r) {
            this.lobTask = lobTask;
            this.r = r;
        }

        @Override
        public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
        }

        @Override
        public int write(Map<StorageMap<Object, ?>, DataBuffer> logs,
                Map<String, StorageMap<?, ?>> maps) {
            lobTask.run();
            return r.write(logs, maps);
        }
    }
}
