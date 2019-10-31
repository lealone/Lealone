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
package org.lealone.transaction.amte.log;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.transaction.amte.AMTransactionEngine;

public abstract class RedoLogRecord {

    private static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private static byte TYPE_CHECKPOINT = 0;
    private static byte TYPE_DROPPED_MAP_REDO_LOG_RECORD = 1;
    private static byte TYPE_LOCAL_TRANSACTION_REDO_LOG_RECORD = 2;
    private static byte TYPE_DISTRIBUTED_TRANSACTION_REDO_LOG_RECORD = 3;

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

    void writeHead(DataBuffer buff) {
    }

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
        } else {
            throw DbException.throwInternalError("unknow type: " + type);
        }
    }

    public static Checkpoint createCheckpoint(long checkpointId) {
        return new Checkpoint(checkpointId);
    }

    public static DroppedMapRedoLogRecord createDroppedMapRedoLogRecord(String mapName) {
        return new DroppedMapRedoLogRecord(mapName);
    }

    public static LocalTransactionRedoLogRecord createLocalTransactionRedoLogRecord(long transactionId,
            ByteBuffer operations) {
        return new LocalTransactionRedoLogRecord(transactionId, operations);
    }

    public static DistributedTransactionRedoLogRecord createDistributedTransactionRedoLogRecord(long transactionId,
            String transactionName, String allLocalTransactionNames, long commitTimestamp, ByteBuffer operations) {

        return new DistributedTransactionRedoLogRecord(transactionId, transactionName, allLocalTransactionNames,
                commitTimestamp, operations);
    }

    public static LazyTransactionRedoLogRecord createLazyTransactionRedoLogRecord(AMTransactionEngine transactionEngine,
            long transactionId, UndoLog undoLog) {
        return new LazyTransactionRedoLogRecord(transactionEngine, transactionId, undoLog);
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
                throw DbException.throwInternalError(
                        "checkpointId=" + checkpointId + ", lastTransactionId=" + lastTransactionId);
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
            DataUtils.checkArgument(mapName != null, "The mapName may not be null");
            this.mapName = mapName;
        }

        @Override
        public long initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog, long lastTransactionId) {
            List<ByteBuffer> logs = pendingRedoLog.get(mapName);
            if (logs != null) {
                logs = new ArrayList<>();
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
                UndoLogRecord.readForRedo(buff, pendingRedoLog);
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

        public LocalTransactionRedoLogRecord(long transactionId, ByteBuffer operations) {
            super(transactionId, operations);
        }

        @Override
        public void write(DataBuffer buff) {
            write(buff, TYPE_LOCAL_TRANSACTION_REDO_LOG_RECORD);
        }

        @Override
        public void writeHead(DataBuffer buff) {
            buff.put(TYPE_LOCAL_TRANSACTION_REDO_LOG_RECORD);
            buff.putVarLong(transactionId);
        }

        public static LocalTransactionRedoLogRecord read(ByteBuffer buff) {
            long transactionId = DataUtils.readVarLong(buff);
            ByteBuffer operations = readOperations(buff);
            return new LocalTransactionRedoLogRecord(transactionId, operations);
        }
    }

    static class DistributedTransactionRedoLogRecord extends TransactionRedoLogRecord {

        private final String transactionName;
        private final String allLocalTransactionNames;
        private final long commitTimestamp;

        public DistributedTransactionRedoLogRecord(long transactionId, String transactionName,
                String allLocalTransactionNames, long commitTimestamp, ByteBuffer operations) {
            super(transactionId, operations);
            this.transactionName = transactionName;
            this.allLocalTransactionNames = allLocalTransactionNames;
            this.commitTimestamp = commitTimestamp;
        }

        @Override
        public void write(DataBuffer buff) {
            write(buff, TYPE_DISTRIBUTED_TRANSACTION_REDO_LOG_RECORD);
            ValueString.type.write(buff, transactionName);
            ValueString.type.write(buff, allLocalTransactionNames);
            buff.putVarLong(commitTimestamp);
        }

        public static DistributedTransactionRedoLogRecord read(ByteBuffer buff) {
            long transactionId = DataUtils.readVarLong(buff);
            ByteBuffer operations = readOperations(buff);
            String transactionName = ValueString.type.read(buff);
            String allLocalTransactionNames = ValueString.type.read(buff);
            long commitTimestamp = DataUtils.readVarLong(buff);
            return new DistributedTransactionRedoLogRecord(transactionId, transactionName, allLocalTransactionNames,
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
            RedoLogRecord redoLogRecord = createLocalTransactionRedoLogRecord(transactionId, null);
            redoLogRecord.writeHead(buffer);
            int pos = buffer.position();
            buffer.putInt(0);
            for (UndoLogRecord r : undoLog.getUndoLogRecords()) {
                r.writeForRedo(buffer, transactionEngine);
            }
            int length = buffer.position() - pos - 4;
            buffer.putInt(pos, length);
        }

        @Override
        long initPendingRedoLog(Map<String, List<ByteBuffer>> pendingRedoLog, long lastTransactionId) {
            throw DbException.throwInternalError();
        }
    }
}
