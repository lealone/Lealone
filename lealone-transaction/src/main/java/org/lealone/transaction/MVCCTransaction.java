/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

/**
 * A transaction.
 */
public class MVCCTransaction implements Transaction {

    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    final MVCCTransactionEngine transactionEngine;
    final int transactionId;
    final String transactionName;

    /**
     * The log id of the last entry in the undo log map.
     */
    long logId;

    Validator validator;

    private int status;
    private boolean autoCommit;
    private boolean local = true; // 默认是true，如果是分布式事务才设为false

    private long commitTimestamp;

    private HashMap<String, Long> savepoints;

    // 协调者或参与者自身的本地事务名
    private StringBuilder localTransactionNamesBuilder;
    // 如果本事务是协调者中的事务，那么在此字段中存放其他参与者的本地事务名
    private ConcurrentSkipListSet<String> participantLocalTransactionNames;
    private List<Participant> participants;

    MVCCTransaction(MVCCTransactionEngine engine, int tid, int status, long logId) {
        transactionEngine = engine;
        transactionId = tid;
        transactionName = getTransactionName(engine.hostAndPort, tid);

        this.status = status;
        this.logId = logId;
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
        StorageMap<K, VersionedValue> map = storage.openMap(name, mapType, keyType, new VersionedValueType(valueType),
                null);
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
    public long getSavepointId() {
        return logId;
    }

    /**
     * Commit the transaction. Afterwards, this transaction is closed.
     */
    @Override
    public void commit() {
        if (local) {
            commitLocal();
            endTransaction();
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

        endTransaction();
    }

    private void commitLocal() {
        checkNotClosed();
        transactionEngine.commit(this, logId);
    }

    private void commitLocalAndTransactionStatusTable(String allLocalTransactionNames) {
        commitLocal();
        transactionEngine.commitTransactionStatusTable(this, allLocalTransactionNames);
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

    private void endTransaction() {
        savepoints = null;
        status = STATUS_CLOSED;
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

    /**
     * Roll the transaction back. Afterwards, this transaction is closed.
     */
    @Override
    public void rollback() {
        try {
            checkNotClosed();
            transactionEngine.rollbackTo(this, logId, 0);
            transactionEngine.endTransaction(this);
        } finally {
            endTransaction();
        }
    }

    @Override
    public void rollbackToSavepoint(String name) {
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }

        Long savepointId = savepoints.get(name);
        if (savepointId == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        long i = savepointId.longValue();
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

    /**
     * Roll back to the given savepoint. This is only allowed if the
     * transaction is open.
     *
     * @param savepointId the savepoint id
     */
    @Override
    public void rollbackToSavepoint(long savepointId) {
        checkNotClosed();
        transactionEngine.rollbackTo(this, logId, savepointId);
        logId = savepointId;
    }

    long getCommitTimestamp() {
        return commitTimestamp;
    }

    void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    /**
     * Create a new savepoint.
     *
     * @return the savepoint id
     */
    public long setSavepoint() {
        return logId;
    }

    /**
     * Add a log entry.
     *
     * @param mapId the map id
     * @param key the key
     * @param oldValue the old value
     */
    void log(int mapId, Object key, Object oldValue) {
        transactionEngine.log(this, logId, mapId, key, oldValue);
        // only increment the log id if logging was successful
        logId++;
    }

    /**
     * Remove the last log entry.
     */
    void logUndo() {
        transactionEngine.logUndo(this, --logId);
    }

    /**
     * Get the list of changes, starting with the latest change, up to the
     * given savepoint (in reverse order than they occurred). The value of
     * the change is the value before the change was applied.
     *
     * @param savepointId the savepoint id, 0 meaning the beginning of the
     *            transaction
     * @return the changes
     */
    // TODO 目前未使用，H2里面的作用主要是通知触发器
    public Iterator<Change> getChanges(long savepointId) {
        return transactionEngine.getChanges(this, logId, savepointId);
    }

    /**
     * Check whether this transaction is open or prepared.
     */
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
