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
package org.lealone.transaction.amte;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.Session;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.value.ValueLong;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.amte.AMTransactionMap.AMReplicationMap;
import org.lealone.transaction.amte.log.LogSyncService;
import org.lealone.transaction.amte.log.RedoLogRecord;

public class AMTransaction implements Transaction {

    // 以下几个public或包级别的字段是在其他地方频繁使用的，
    // 为了使用方便或节省一点点性能开销就不通过getter方法访问了
    final AMTransactionEngine transactionEngine;
    public final long transactionId;
    public final String transactionName;

    String globalTransactionName;
    int logId;
    LinkedList<TransactionalLogRecord> logRecords = new LinkedList<>();

    private final LogSyncService logSyncService;

    private HashMap<String, Integer> savepoints;
    private Session session;
    private int status;
    private boolean autoCommit;
    private boolean prepared;

    public AMTransaction(AMTransactionEngine engine, long tid) {
        this(engine, tid, null);
    }

    public AMTransaction(AMTransactionEngine engine, long tid, String hostAndPort) {
        transactionEngine = engine;
        transactionId = tid;
        transactionName = getTransactionName(hostAndPort, tid);
        logSyncService = engine.getLogSyncService();
        status = Transaction.STATUS_OPEN;
    }

    public void log(String mapName, Object key, TransactionalValue oldValue, TransactionalValue newValue) {
        logRecords.add(new TransactionalLogRecord(mapName, key, oldValue, newValue));
        logId++;
    }

    public void logUndo() {
        logRecords.removeLast();
        --logId;
    }

    @Override
    public String getGlobalTransactionName() {
        return globalTransactionName;
    }

    @Override
    public void setGlobalTransactionName(String globalTransactionName) {
        this.globalTransactionName = globalTransactionName;
    }

    @Override
    public void setSession(Session session) {
        this.session = session;
    }

    public Session getSession() {
        return session;
    }

    public boolean isShardingMode() {
        return session != null && !session.isLocal() && session.isShardingMode();
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public void setLocal(boolean local) {
    }

    public boolean isLocal() {
        return true;
    }

    @Override
    public void addLocalTransactionNames(String localTransactionNames) {
    }

    @Override
    public String getLocalTransactionNames() {
        return null;
    }

    @Override
    public void setValidator(Validator validator) {
    }

    @Override
    public void addParticipant(Participant participant) {
    }

    @Override
    public <K, V> AMTransactionMap<K, V> openMap(String name, Storage storage) {
        return openMap(name, null, null, storage);
    }

    @Override
    public <K, V> AMTransactionMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType,
            Storage storage) {
        return openMap(name, keyType, valueType, storage, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> AMTransactionMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType,
            Storage storage, Map<String, String> parameters) {
        checkNotClosed();
        if (keyType == null)
            keyType = new ObjectDataType();
        if (valueType == null)
            valueType = new ObjectDataType();
        valueType = new TransactionalValueType(valueType);
        StorageMap<K, TransactionalValue> map = storage.openMap(name, keyType, valueType, parameters);
        if (!map.isInMemory()) {
            TransactionalLogRecord.redo(map, logSyncService.getAndRemovePendingRedoLog(map.getName()));
        }
        transactionEngine.addMap((StorageMap<Object, TransactionalValue>) map);
        boolean isShardingMode = parameters == null ? false : Boolean.parseBoolean(parameters.get("isShardingMode"));
        return createTransactionMap(map, isShardingMode);
    }

    protected <K, V> AMTransactionMap<K, V> createTransactionMap(StorageMap<K, TransactionalValue> map,
            boolean isShardingMode) {
        if (isShardingMode)
            return new AMReplicationMap<>(this, map);
        else
            return new AMTransactionMap<>(this, map);
    }

    @Override
    public void addSavepoint(String name) {
        if (savepoints == null)
            savepoints = new HashMap<>();

        savepoints.put(name, getSavepointId());
    }

    @Override
    public int getSavepointId() {
        return logId;
    }

    @Override
    public void prepareCommit() {
        checkNotClosed();
        prepared = true;

        RedoLogRecord r = createLocalTransactionRedoLogRecord();
        // 事务没有进行任何操作时不用同步日志
        if (r != null) {
            // 先写redoLog
            logSyncService.addRedoLogRecord(r);
        }
        logSyncService.prepareCommit(this);
    }

    @Override
    public void prepareCommit(String allLocalTransactionNames) {
        prepareCommit();
    }

    @Override
    public void commit() {
        commitLocal();
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        commitLocal();
    }

    protected void commitLocal() {
        checkNotClosed();
        if (prepared) { // 在prepareCommit阶段已经写完redoLog了
            commitFinal();
            if (session != null && session.getRunnable() != null) {
                try {
                    session.getRunnable().run();
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
        } else {
            RedoLogRecord r = createLocalTransactionRedoLogRecord();
            if (r != null) { // 事务没有进行任何操作时不用同步日志
                // 先写redoLog
                logSyncService.addAndMaybeWaitForSync(r);
            }
            // 分布式事务推迟提交
            if (isLocal()) {
                commitFinal();
            }
        }
    }

    protected void commitFinal() {
        commitFinal(transactionId);
    }

    // tid在分布式场景下可能是其他事务的tid
    protected void commitFinal(long tid) {
        // 避免并发提交(TransactionValidator线程和其他读写线程都有可能在检查到分布式事务有效后帮助提交最终事务)
        AMTransaction t = transactionEngine.removeTransaction(tid);
        if (t == null)
            return;
        for (TransactionalLogRecord r : t.logRecords) {
            r.commit(transactionEngine, tid);
        }
        t.endTransaction();
    }

    private void endTransaction() {
        savepoints = null;
        logRecords = null;
        status = STATUS_CLOSED;
        transactionEngine.removeTransaction(transactionId);
    }

    // 将当前一系列的事务操作日志转换成单条RedoLogRecord
    protected ByteBuffer logRecords2redoLogRecordBuffer() {
        if (logRecords.isEmpty())
            return null;
        try (DataBuffer writeBuffer = DataBuffer.create()) {
            for (TransactionalLogRecord r : logRecords) {
                r.writeForRedo(writeBuffer, transactionEngine);
            }
            ByteBuffer buffer = writeBuffer.getAndFlipBuffer();
            ByteBuffer operations = ByteBuffer.allocateDirect(buffer.limit());
            operations.put(buffer);
            operations.flip();
            return operations;
        }
    }

    private RedoLogRecord createLocalTransactionRedoLogRecord() {
        ByteBuffer operations = logRecords2redoLogRecordBuffer();
        if (operations == null)
            return null;
        return RedoLogRecord.createLocalTransactionRedoLogRecord(transactionId, operations);
    }

    @Override
    public void rollback() {
        try {
            checkNotClosed();
            rollbackTo(0);
        } finally {
            endTransaction();
        }
    }

    @Override
    public void rollbackToSavepoint(String name) {
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }

        Integer savepointId = savepoints.get(name);
        if (savepointId == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        int i = savepointId.intValue();
        rollbackToSavepoint(i);

        if (savepoints != null) {
            String[] names = new String[savepoints.size()];
            savepoints.keySet().toArray(names);
            for (String n : names) {
                savepointId = savepoints.get(n);
                if (savepointId.longValue() >= i) {
                    savepoints.remove(n);
                }
            }
        }
    }

    @Override
    public void rollbackToSavepoint(int savepointId) {
        checkNotClosed();
        rollbackTo(savepointId);
        logId = savepointId;
    }

    private void rollbackTo(long toLogId) {
        while (--logId >= toLogId) {
            TransactionalLogRecord r = logRecords.removeLast();
            r.rollback(transactionEngine);
        }
    }

    protected void checkNotClosed() {
        if (status == STATUS_CLOSED) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "Transaction is closed");
        }
    }

    @Override
    public String toString() {
        return "AMT[" + transactionName + ", " + autoCommit + "]";
    }

    public static String getTransactionName(String hostAndPort, long tid) {
        if (hostAndPort == null)
            hostAndPort = "0:0";
        StringBuilder buff = new StringBuilder(hostAndPort);
        buff.append(':');
        buff.append(tid);
        return buff.toString();
    }

    private Object lastKey;
    private TransactionalValue lastValue;
    private StorageMap<Object, TransactionalValue> lastStorageMap;

    @Override
    public void replicationPrepareCommit(long validKey) {
        if (lastValue != null && validKey != -1) {
            Object key = ValueLong.get(validKey);
            TransactionalValue newValue = lastStorageMap.get(lastKey);
            if (newValue == lastValue) {
                lastStorageMap.remove(lastKey);
            }
            lastStorageMap.put(key, lastValue);
            logRecords.getLast().key = key; // 替换原来的key
        }
    }

    void logAppend(StorageMap<Object, TransactionalValue> map, Object key, TransactionalValue newValue) {
        log(map.getName(), key, null, newValue);
        lastKey = key;
        lastValue = newValue;
        lastStorageMap = map;
    }
}
