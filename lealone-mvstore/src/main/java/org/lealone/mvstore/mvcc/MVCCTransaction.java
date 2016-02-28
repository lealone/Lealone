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
package org.lealone.mvstore.mvcc;

import java.util.HashMap;
import java.util.LinkedList;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.Session;
import org.lealone.mvstore.mvcc.log.RedoLogValue;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.transaction.Transaction;

public class MVCCTransaction implements Transaction {

    final MVCCTransactionEngine transactionEngine;
    final long transactionId;
    final String transactionName;

    int logId;

    private int status;
    private boolean autoCommit;

    private long commitTimestamp;

    private HashMap<String, Integer> savepoints;

    LinkedList<LogRecord> logRecords = new LinkedList<>();

    MVCCTransaction(MVCCTransactionEngine engine, long tid) {
        transactionEngine = engine;
        transactionId = tid;
        transactionName = getTransactionName(null, tid);
        status = MVCCTransaction.STATUS_OPEN;
    }

    static class LogRecord {
        final String mapName;
        final Object key;
        final VersionedValue oldValue;
        final VersionedValue newValue;

        public LogRecord(String mapName, Object key, VersionedValue oldValue, VersionedValue newValue) {
            this.mapName = mapName;
            this.key = key;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }
    }

    void log(String mapName, Object key, VersionedValue oldValue, VersionedValue newValue) {
        logRecords.add(new LogRecord(mapName, key, oldValue, newValue));
        logId++;
    }

    void logUndo() {
        logRecords.removeLast();
        --logId;
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
    public <K, V> MVCCTransactionMap<K, V> openMap(String name, Storage storage) {
        return openMap(name, null, null, storage);
    }

    @Override
    public <K, V> MVCCTransactionMap<K, V> openMap(String name, DataType keyType, DataType valueType, Storage storage) {
        return openMap(name, null, keyType, valueType, storage, false);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> MVCCTransactionMap<K, V> openMap(String name, String mapType, DataType keyType, DataType valueType,
            Storage storage, boolean isShardingMode) {
        if (keyType == null)
            keyType = new ObjectDataType();
        if (valueType == null)
            valueType = new ObjectDataType();

        checkNotClosed();
        valueType = new VersionedValueType(valueType);
        StorageMap<K, VersionedValue> map = storage.openMap(name, mapType, keyType, valueType, null);
        transactionEngine.redo(map);
        transactionEngine.addMap((StorageMap<Object, VersionedValue>) map);
        return new MVCCTransactionMap<>(this, map);
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
    public void commit() {
        commitLocal();
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        commitLocal();
    }

    void endTransaction() {
        savepoints = null;
        logRecords = null;
        status = STATUS_CLOSED;

        transactionEngine.currentTransactions.remove(transactionId);
    }

    private void commitLocal() {
        checkNotClosed();
        RedoLogValue v = transactionEngine.getRedoLog(this);
        transactionEngine.commit(this, v);
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

    long getCommitTimestamp() {
        return commitTimestamp;
    }

    void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    private void rollbackTo(long toLogId) {
        while (--logId >= toLogId) {
            LogRecord r = logRecords.removeLast();
            String mapName = r.mapName;
            StorageMap<Object, VersionedValue> map = transactionEngine.getMap(mapName);
            if (map != null) {
                Object key = r.key;
                VersionedValue oldValue = r.oldValue;
                if (oldValue == null) {
                    // this transaction added the value
                    map.remove(key);
                } else {
                    // this transaction updated the value
                    map.put(key, oldValue);
                }
            }
        }

    }

    void checkNotClosed() {
        if (status == STATUS_CLOSED) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "Transaction is closed");
        }
    }

    @Override
    public String toString() {
        return "" + transactionId;
    }

    static String getTransactionName(String hostAndPort, long tid) {
        if (hostAndPort == null)
            hostAndPort = "0:0";
        StringBuilder buff = new StringBuilder(hostAndPort);
        buff.append(':');
        buff.append(tid);
        return buff.toString();
    }

    @Override
    public String getGlobalTransactionName() {
        return null;
    }

    @Override
    public void setGlobalTransactionName(String globalTransactionName) {
    }

    @Override
    public void setSession(Session session) {
    }
}
