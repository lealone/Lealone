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
package org.lealone.mvcc;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.Session;
import org.lealone.mvcc.log.RedoLogValue;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.transaction.Transaction;

public class MVCCTransaction implements Transaction {

    final MVCCTransactionEngine transactionEngine;
    public final long transactionId;
    public final String transactionName;

    String globalTransactionName;
    int logId;
    int status;
    boolean autoCommit;
    protected long commitTimestamp;
    protected Session session;

    private HashMap<String, Integer> savepoints;

    LinkedList<LogRecord> logRecords = new LinkedList<>();

    public MVCCTransaction(MVCCTransactionEngine engine, long tid) {
        this(engine, tid, null);
    }

    public MVCCTransaction(MVCCTransactionEngine engine, long tid, String hostAndPort) {
        transactionEngine = engine;
        transactionId = tid;
        transactionName = getTransactionName(hostAndPort, tid);
        status = Transaction.STATUS_OPEN;
    }

    static class LogRecord {
        final String mapName;
        final Object key;
        final TransactionalValue oldValue;
        final TransactionalValue newValue;

        public LogRecord(String mapName, Object key, TransactionalValue oldValue, TransactionalValue newValue) {
            this.mapName = mapName;
            this.key = key;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }
    }

    public void log(String mapName, Object key, TransactionalValue oldValue, TransactionalValue newValue) {
        logRecords.add(new LogRecord(mapName, key, oldValue, newValue));
        logId++;
    }

    public void logUndo() {
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
    public <K, V> MVCCTransactionMap<K, V> openMap(String name, Storage storage) {
        return openMap(name, null, null, storage);
    }

    @Override
    public <K, V> MVCCTransactionMap<K, V> openMap(String name, DataType keyType, DataType valueType, Storage storage) {
        return openMap(name, null, keyType, valueType, storage, false, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> MVCCTransactionMap<K, V> openMap(String name, String mapType, DataType keyType, DataType valueType,
            Storage storage, boolean isShardingMode, String initReplicationEndpoints) {
        if (keyType == null)
            keyType = new ObjectDataType();
        if (valueType == null)
            valueType = new ObjectDataType();

        checkNotClosed();
        valueType = new TransactionalValueType(valueType);
        Map<String, String> parameters = new HashMap<>(1);
        if (isShardingMode) {
            mapType = "BTreeMap";
            parameters.put("isShardingMode", "true");
            parameters.put("initReplicationEndpoints", initReplicationEndpoints);
        }
        StorageMap<K, TransactionalValue> map = storage.openMap(name, mapType, keyType, valueType, parameters);
        transactionEngine.redo(map);
        transactionEngine.addMap((StorageMap<Object, TransactionalValue>) map);
        return createTransactionMap(map);
    }

    protected <K, V> MVCCTransactionMap<K, V> createTransactionMap(StorageMap<K, TransactionalValue> map) {
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

    private boolean prepared;

    @Override
    public void prepareCommit() {
        checkNotClosed();
        prepared = true;
        RedoLogValue v = transactionEngine.getRedoLog(this);
        transactionEngine.prepareCommit(this, v);
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

    void endTransaction() {
        savepoints = null;
        logRecords = null;
        status = STATUS_CLOSED;

        transactionEngine.currentTransactions.remove(transactionId);
    }

    protected void commitLocal() {
        checkNotClosed();
        if (prepared) {
            transactionEngine.commit(this);
        } else {
            RedoLogValue v = transactionEngine.getRedoLog(this);
            transactionEngine.commit(this, v);
        }
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

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    protected void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    private void rollbackTo(long toLogId) {
        while (--logId >= toLogId) {
            LogRecord r = logRecords.removeLast();
            String mapName = r.mapName;
            StorageMap<Object, TransactionalValue> map = transactionEngine.getMap(mapName);
            if (map != null) {
                Object key = r.key;
                TransactionalValue oldValue = r.oldValue;
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

    protected void checkNotClosed() {
        if (status == STATUS_CLOSED) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "Transaction is closed");
        }
    }

    @Override
    public String toString() {
        return "" + transactionId;
    }

    public static String getTransactionName(String hostAndPort, long tid) {
        if (hostAndPort == null)
            hostAndPort = "0:0";
        StringBuilder buff = new StringBuilder(hostAndPort);
        buff.append(':');
        buff.append(tid);
        return buff.toString();
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
}
