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
package org.lealone.transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.New;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.transaction.log.RedoLogValue;

public class MVCCTransaction implements Transaction {

    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    final MVCCTransactionEngine transactionEngine;
    final long transactionId;
    final String transactionName;

    int logId;

    Validator validator;

    private int status;
    private boolean autoCommit;
    private boolean local = true; // 默认是true，如果是分布式事务才设为false

    private long commitTimestamp;

    private HashMap<String, Integer> savepoints;

    // 协调者或参与者自身的本地事务名
    private StringBuilder localTransactionNamesBuilder;
    // 如果本事务是协调者中的事务，那么在此字段中存放其他参与者的本地事务名
    private ConcurrentSkipListSet<String> participantLocalTransactionNames;
    private List<Participant> participants;

    LinkedList<LogRecord> logRecords = new LinkedList<>();

    MVCCTransaction(MVCCTransactionEngine engine, long tid, int status, int logId) {
        transactionEngine = engine;
        transactionId = tid;
        transactionName = getTransactionName(engine.hostAndPort, tid);

        this.status = status;
        this.logId = logId;
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
        this.local = local;
    }

    /**
     * 假设有RS1、RS2、RS3，Client启动的一个事务涉及这三个RS, 
     * 第一个接收到Client读写请求的RS即是协调者也是参与者，之后Client的任何读写请求都只会跟协调者打交道，
     * 假设这里的协调者是RS1，当读写由RS1转发到RS2时，RS2在完成读写请求后会把它的本地事务名(可能有多个(嵌套事务)发回来，
     * 此时协调者必须记下所有其他参与者的本地事务名。<p>
     * 
     * 如果本地事务名是null，代表参与者执行完读写请求后发现跟上次的本地事务名一样，为了减少网络传输就不再重发。
     */
    @Override
    public void addLocalTransactionNames(String localTransactionNames) {
        if (localTransactionNames != null) {
            if (participantLocalTransactionNames == null)
                participantLocalTransactionNames = new ConcurrentSkipListSet<>();
            for (String name : localTransactionNames.split(","))
                participantLocalTransactionNames.add(name.trim());
        }
    }

    @Override
    public String getLocalTransactionNames() {
        StringBuilder buff = new StringBuilder(transactionName);

        if (participantLocalTransactionNames != null) {
            for (String name : participantLocalTransactionNames) {
                buff.append(',');
                buff.append(name);
            }
        }

        if (localTransactionNamesBuilder != null && localTransactionNamesBuilder.equals(buff))
            return null;
        localTransactionNamesBuilder = buff;
        return buff.toString();
    }

    @Override
    public void setValidator(Validator validator) {
        this.validator = validator;
    }

    @Override
    public void addParticipant(Participant participant) {
        if (participants == null)
            participants = new ArrayList<>();
        participants.add(participant);
    }

    @Override
    public <K, V> MVCCTransactionMap<K, V> openMap(String name, Storage storage) {
        return openMap(name, null, null, storage);
    }

    @Override
    public <K, V> MVCCTransactionMap<K, V> openMap(String name, DataType keyType, DataType valueType, Storage storage) {
        return openMap(name, null, keyType, valueType, storage);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> MVCCTransactionMap<K, V> openMap(String name, String mapType, DataType keyType, DataType valueType,
            Storage storage) {
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

        if (participants != null && !isAutoCommit())
            parallelSavepoint(true, name);
    }

    @Override
    public int getSavepointId() {
        return logId;
    }

    @Override
    public void commit() {
        if (local) {
            commitLocal();
        } else
            commit(null);
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        if (allLocalTransactionNames == null)
            allLocalTransactionNames = getAllLocalTransactionNames();
        List<Future<Void>> futures = null;
        if (participants != null && !isAutoCommit())
            futures = parallelCommitOrRollback(allLocalTransactionNames);

        commitLocalAndTransactionStatusTable(allLocalTransactionNames);
        if (futures != null)
            waitFutures(futures);
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

    private void commitLocalAndTransactionStatusTable(String allLocalTransactionNames) {
        checkNotClosed();
        RedoLogValue v = transactionEngine.getRedoLog(this);
        setCommitTimestamp(transactionEngine.nextOddTransactionId());
        v.transactionName = transactionName;
        v.allLocalTransactionNames = allLocalTransactionNames;
        v.commitTimestamp = commitTimestamp;
        transactionEngine.commit(this, v);

        TransactionStatusTable.put(this, allLocalTransactionNames);
        TransactionValidator.enqueue(this, allLocalTransactionNames);
    }

    private void waitFutures(List<Future<Void>> futures) {
        try {
            for (int i = 0, size = futures.size(); i < size; i++) {
                futures.get(i).get();
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    boolean isLocal() {
        return transactionId % 2 == 0;
    }

    private List<Future<Void>> parallelCommitOrRollback(final String allLocalTransactionNames) {
        int size = participants.size();
        List<Future<Void>> futures = New.arrayList(size);
        for (final Participant participant : participants) {
            futures.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (allLocalTransactionNames != null)
                        participant.commitTransaction(allLocalTransactionNames);
                    else
                        participant.rollbackTransaction();
                    return null;
                }
            }));
        }
        return futures;
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

        if (participants != null && !isAutoCommit())
            parallelSavepoint(false, name);
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

    private void parallelSavepoint(final boolean add, final String name) {
        int size = participants.size();
        List<Future<Void>> futures = New.arrayList(size);
        for (final Participant participant : participants) {
            futures.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (add)
                        participant.addSavepoint(name);
                    else
                        participant.rollbackToSavepoint(name);
                    return null;
                }
            }));
        }
        try {
            for (int i = 0; i < size; i++) {
                futures.get(i).get();
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private String getAllLocalTransactionNames() {
        getLocalTransactionNames();
        return localTransactionNamesBuilder.toString();
    }

    static String getTransactionName(String hostAndPort, long tid) {
        if (hostAndPort == null)
            hostAndPort = "0:0";
        StringBuilder buff = new StringBuilder(hostAndPort);
        buff.append(':');
        buff.append(tid);
        return buff.toString();
    }
}
