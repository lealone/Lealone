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

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.storage.replication.ReplicationConflictType;
import org.lealone.transaction.aote.AMTransactionEngine;

public abstract class RedoLogRecord {

    private static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private static byte TYPE_CHECKPOINT = 0;
    private static byte TYPE_DROPPED_MAP_REDO_LOG_RECORD = 1;
    private static byte TYPE_LOCAL_TRANSACTION_REDO_LOG_RECORD = 2;
    private static byte TYPE_DISTRIBUTED_TRANSACTION_REDO_LOG_RECORD = 3;
    private static byte TYPE_REPLICA_PREPARE_COMMIT_REDO_LOG_RECORD = 4;
    private static byte TYPE_REPLICA_COMMIT_REDO_LOG_RECORD = 5;

    private volatile boolean synced;

    boolean isSynced() {
        return synced;
    }

    void setSynced(boolean synced) {
        this.synced = synced;
    }

    boolean isCheckpoint() {
        return false;
    }

    abstract long initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog, long lastTransactionId);

    abstract void write(DataBuffer buff);

    static RedoLogRecord read(ByteBuffer buff) {
        int type = buff.get();
        if (type == TYPE_CHECKPOINT) {
            return Checkpoint.read(buff);
        } else if (type == TYPE_DROPPED_MAP_REDO_LOG_RECORD) {
            return DroppedMapRedoLogRecord.read(buff);
        } else if (type == TYPE_LOCAL_TRANSACTION_REDO_LOG_RECORD) {
            return LocalTransactionRedoLogRecord.read(buff);
        } else if (type == TYPE_DISTRIBUTED_TRANSACTION_REDO_LOG_RECORD) {
            return DistributedTransactionRedoLogRecord.read(buff);
        } else if (type == TYPE_REPLICA_PREPARE_COMMIT_REDO_LOG_RECORD) {
            return ReplicaPrepareCommitRedoLogRecord.read(buff);
        } else if (type == TYPE_REPLICA_COMMIT_REDO_LOG_RECORD) {
            return ReplicaCommitRedoLogRecord.read(buff);
        } else {
            throw DbException.getInternalError("unknow type: " + type);
        }
    }

    public static Checkpoint createCheckpoint(long checkpointId) {
        return new Checkpoint(checkpointId);
    }

    public static DroppedMapRedoLogRecord createDroppedMapRedoLogRecord(String mapName) {
        return new DroppedMapRedoLogRecord(mapName);
    }

    public static LocalTransactionRedoLogRecord createLocalTransactionRedoLogRecord(long transactionId,
            DataBuffer operations) {
        return new LocalTransactionRedoLogRecord(transactionId, operations);
    }

    public static DistributedTransactionRedoLogRecord createDistributedTransactionRedoLogRecord(long transactionId,
            String transactionName, String globalTransactionName, long commitTimestamp, DataBuffer operations) {

        return new DistributedTransactionRedoLogRecord(transactionId, transactionName, globalTransactionName,
                commitTimestamp, operations);
    }

    public static LazyTransactionRedoLogRecord createLazyTransactionRedoLogRecord(AMTransactionEngine transactionEngine,
            long transactionId, UndoLog undoLog) {
        return new LazyTransactionRedoLogRecord(transactionEngine, transactionId, undoLog);
    }

    public static ReplicaPrepareCommitRedoLogRecord createReplicaPrepareCommitRedoLogRecord(String sql, int updateCount,
            long first, String uncommittedReplicationName, String currentReplicationName,
            ReplicationConflictType replicationConflictType) {
        return new ReplicaPrepareCommitRedoLogRecord(sql, updateCount, first, uncommittedReplicationName,
                currentReplicationName, replicationConflictType);
    }

    public static ReplicaCommitRedoLogRecord createReplicaCommitRedoLogRecord(String currentReplicationName) {
        return new ReplicaCommitRedoLogRecord(currentReplicationName);
    }

    static class Checkpoint extends RedoLogRecord {

        private final long checkpointId;

        Checkpoint(long checkpointId) {
            this.checkpointId = checkpointId;
        }

        @Override
        public boolean isCheckpoint() {
            return true;
        }

        @Override
        public long initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog, long lastTransactionId) {
            pendingRedoLog.clear();
            if (checkpointId < lastTransactionId) {
                throw DbException
                        .getInternalError("checkpointId=" + checkpointId + ", lastTransactionId=" + lastTransactionId);
            }
            return checkpointId;
        }

        @Override
        public void write(DataBuffer buff) {
            buff.put(TYPE_CHECKPOINT);
            buff.putVarLong(checkpointId);
        }

        public static RedoLogRecord read(ByteBuffer buff) {
            long checkpointId = DataUtils.readVarLong(buff);
            return new Checkpoint(checkpointId);
        }
    }

    static class DroppedMapRedoLogRecord extends RedoLogRecord {

        private final String mapName;

        DroppedMapRedoLogRecord(String mapName) {
            DataUtils.checkNotNull(mapName, "mapName");
            this.mapName = mapName;
        }

        @Override
        public long initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog, long lastTransactionId) {
            List<ByteBuffer> logs = pendingRedoLog.get(mapName);
            if (logs != null) {
                logs = new LinkedList<>();
                pendingRedoLog.put(mapName, logs);
            }
            return lastTransactionId;
        }

        @Override
        public void write(DataBuffer buff) {
            buff.put(TYPE_DROPPED_MAP_REDO_LOG_RECORD);
            ValueString.type.write(buff, mapName);
        }

        public static RedoLogRecord read(ByteBuffer buff) {
            String mapName = ValueString.type.read(buff);
            return new DroppedMapRedoLogRecord(mapName);
        }
    }

    static class TransactionRedoLogRecord extends RedoLogRecord {

        protected final long transactionId;
        protected final ByteBuffer operations;

        public TransactionRedoLogRecord(long transactionId, ByteBuffer operations) {
            this.transactionId = transactionId;
            this.operations = operations;
        }

        @Override
        public long initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog, long lastTransactionId) {
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
            return transactionId > lastTransactionId ? transactionId : lastTransactionId;
        }

        @Override
        public void write(DataBuffer buff) {
            write(buff, TYPE_LOCAL_TRANSACTION_REDO_LOG_RECORD);
        }

        public void write(DataBuffer buff, byte type) {
            buff.put(type);
            buff.putVarLong(transactionId);
            buff.putInt(operations.remaining());
            buff.put(operations);
        }

        public static ByteBuffer readOperations(ByteBuffer buff) {
            ByteBuffer operations;
            int len = buff.getInt(); // DataUtils.readVarInt(buff);
            if (len > 0) {
                byte[] value = new byte[len];
                buff.get(value);
                operations = ByteBuffer.wrap(value);
            } else {
                operations = EMPTY_BUFFER;
            }
            return operations;
        }
    }

    static class LocalTransactionRedoLogRecord extends TransactionRedoLogRecord {

        private DataBuffer buffer;

        public LocalTransactionRedoLogRecord(long transactionId, ByteBuffer operations) {
            super(transactionId, operations);
        }

        public LocalTransactionRedoLogRecord(long transactionId, DataBuffer operations) {
            super(transactionId, operations.getBuffer());
            this.buffer = operations;
        }

        @Override
        public void write(DataBuffer buff) {
            write(buff, TYPE_LOCAL_TRANSACTION_REDO_LOG_RECORD);
            if (buffer != null) {
                buffer.close();
            }
        }

        public static LocalTransactionRedoLogRecord read(ByteBuffer buff) {
            long transactionId = DataUtils.readVarLong(buff);
            ByteBuffer operations = readOperations(buff);
            return new LocalTransactionRedoLogRecord(transactionId, operations);
        }
    }

    static class DistributedTransactionRedoLogRecord extends TransactionRedoLogRecord {

        private final String transactionName;
        private final String globalTransactionName;
        private final long commitTimestamp;
        private DataBuffer buffer;

        public DistributedTransactionRedoLogRecord(long transactionId, String transactionName,
                String globalTransactionName, long commitTimestamp, DataBuffer operations) {
            this(transactionId, transactionName, globalTransactionName, commitTimestamp, operations.getBuffer());
            this.buffer = operations;
        }

        public DistributedTransactionRedoLogRecord(long transactionId, String transactionName,
                String globalTransactionName, long commitTimestamp, ByteBuffer operations) {
            super(transactionId, operations);
            this.transactionName = transactionName;
            this.globalTransactionName = globalTransactionName;
            this.commitTimestamp = commitTimestamp;
        }

        @Override
        public void write(DataBuffer buff) {
            write(buff, TYPE_DISTRIBUTED_TRANSACTION_REDO_LOG_RECORD);
            ValueString.type.write(buff, transactionName);
            ValueString.type.write(buff, globalTransactionName);
            buff.putVarLong(commitTimestamp);
            if (buffer != null) {
                buffer.close();
            }
        }

        public static DistributedTransactionRedoLogRecord read(ByteBuffer buff) {
            long transactionId = DataUtils.readVarLong(buff);
            ByteBuffer operations = readOperations(buff);
            String transactionName = ValueString.type.read(buff);
            String globalTransactionName = ValueString.type.read(buff);
            long commitTimestamp = DataUtils.readVarLong(buff);
            return new DistributedTransactionRedoLogRecord(transactionId, transactionName, globalTransactionName,
                    commitTimestamp, operations);
        }
    }

    static class LazyTransactionRedoLogRecord extends RedoLogRecord {

        final AMTransactionEngine transactionEngine;
        final long transactionId;
        final UndoLog undoLog;

        public LazyTransactionRedoLogRecord(AMTransactionEngine transactionEngine, long transactionId,
                UndoLog undoLog) {
            this.transactionEngine = transactionEngine;
            this.transactionId = transactionId;
            this.undoLog = undoLog;
        }

        @Override
        void write(DataBuffer buffer) {
            if (undoLog.isEmpty())
                return;
            buffer.put(TYPE_LOCAL_TRANSACTION_REDO_LOG_RECORD);
            buffer.putVarLong(transactionId);
            int pos = buffer.position();
            buffer.putInt(0);
            UndoLogRecord r = undoLog.getFirst();
            while (r != null) {
                r.writeForRedo(buffer, transactionEngine);
                r = r.next;
            }
            int length = buffer.position() - pos - 4;
            buffer.putInt(pos, length);
        }

        @Override
        long initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog, long lastTransactionId) {
            throw DbException.getInternalError();
        }
    }

    static class ReplicaPrepareCommitRedoLogRecord extends RedoLogRecord {

        private final String sql;
        private final int updateCount;
        private final long first;
        private final String uncommittedReplicationName;
        private final String currentReplicationName;
        private final ReplicationConflictType replicationConflictType;

        ReplicaPrepareCommitRedoLogRecord(String sql, int updateCount, long first, String uncommittedReplicationName,
                String currentReplicationName, ReplicationConflictType replicationConflictType) {
            this.sql = sql;
            this.updateCount = updateCount;
            this.first = first;
            this.uncommittedReplicationName = uncommittedReplicationName;
            this.currentReplicationName = currentReplicationName;
            this.replicationConflictType = replicationConflictType;
        }

        public String getCurrentReplicationName() {
            return currentReplicationName;
        }

        @Override
        public long initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog, long lastTransactionId) {
            return lastTransactionId;
        }

        @Override
        public void write(DataBuffer buff) {
            buff.put(TYPE_REPLICA_PREPARE_COMMIT_REDO_LOG_RECORD);
            ValueString.type.write(buff, sql);
            buff.putVarInt(updateCount);
            buff.putVarLong(first);
            ValueString.type.write(buff, uncommittedReplicationName);
            ValueString.type.write(buff, currentReplicationName);
            buff.putVarInt(replicationConflictType.value);
        }

        public static RedoLogRecord read(ByteBuffer buff) {
            String sql = ValueString.type.read(buff);
            int updateCount = DataUtils.readVarInt(buff);
            long first = DataUtils.readVarLong(buff);
            String uncommittedReplicationName = ValueString.type.read(buff);
            String currentReplicationName = ValueString.type.read(buff);
            ReplicationConflictType replicationConflictType = ReplicationConflictType
                    .getType(DataUtils.readVarInt(buff));
            return new ReplicaPrepareCommitRedoLogRecord(sql, updateCount, first, uncommittedReplicationName,
                    currentReplicationName, replicationConflictType);
        }

        public void redo() {
            // TODO
        }
    }

    static class ReplicaCommitRedoLogRecord extends RedoLogRecord {

        private final String currentReplicationName;

        ReplicaCommitRedoLogRecord(String currentReplicationName) {
            this.currentReplicationName = currentReplicationName;
        }

        public String getCurrentReplicationName() {
            return currentReplicationName;
        }

        @Override
        public long initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog, long lastTransactionId) {
            return lastTransactionId;
        }

        @Override
        public void write(DataBuffer buff) {
            buff.put(TYPE_REPLICA_COMMIT_REDO_LOG_RECORD);
            ValueString.type.write(buff, currentReplicationName);
        }

        public static RedoLogRecord read(ByteBuffer buff) {
            String currentReplicationName = ValueString.type.read(buff);
            return new ReplicaCommitRedoLogRecord(currentReplicationName);
        }
    }
}
