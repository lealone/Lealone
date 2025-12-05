/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.log;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.value.ValueString;
import com.lealone.storage.FormatVersion;
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageMap.RedoLogBuffer;

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

    public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
    }

    public int write(Map<String, RedoLogBuffer> logs, int logServiceIndex) {
        return 0;
    }

    public Map<StorageMap<?, ?>, AtomicBoolean> getMaps() {
        return null;
    }

    public Set<Integer> getRedoLogServiceIndexs() {
        return null;
    }

    // 兼容老版本的redo log
    private static class CheckpointRLR extends RedoLogRecord {

        @Override
        public boolean isCheckpoint() {
            return true;
        }

        public static RedoLogRecord read(ByteBuffer buff) {
            DataUtils.readVarLong(buff); // checkpointId兼容老版本
            return new CheckpointRLR();
        }
    }

    // 兼容老版本的redo log
    private static class DroppedMapRLR extends RedoLogRecord {

        private final String mapName;

        public DroppedMapRLR(String mapName) {
            DataUtils.checkNotNull(mapName, "mapName");
            this.mapName = mapName;
        }

        @Override
        public void initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog) {
            pendingRedoLog.remove(mapName);
        }

        public static RedoLogRecord read(ByteBuffer buff) {
            String mapName = ValueString.type.read(buff, FormatVersion.FORMAT_VERSION_1);
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
        public int write(Map<String, RedoLogBuffer> logs, int logServiceIndex) {
            return undoLog.writeForRedo(logs, logServiceIndex);
        }

        @Override
        public Map<StorageMap<?, ?>, AtomicBoolean> getMaps() {
            return undoLog.getMaps();
        }

        @Override
        public Set<Integer> getRedoLogServiceIndexs() {
            return undoLog.getRedoLogServiceIndexs();
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
        public Map<StorageMap<?, ?>, AtomicBoolean> getMaps() {
            return r.getMaps();
        }

        @Override
        public Set<Integer> getRedoLogServiceIndexs() {
            return r.getRedoLogServiceIndexs();
        }

        @Override
        public int write(Map<String, RedoLogBuffer> logs, int logServiceIndex) {
            lobTask.run();
            return r.write(logs, logServiceIndex);
        }
    }
}
