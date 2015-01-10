/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction.local;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lealone.command.router.FrontendSessionPool;
import org.lealone.engine.FrontendSession;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.mvstore.DataUtils;
import org.lealone.mvstore.MVMap;
import org.lealone.mvstore.type.DataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionManager;
import org.lealone.util.New;

/**
 * A transaction.
 */
public class LocalTransaction implements Transaction {

    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    /**
     * The status of a closed transaction (committed or rolled back).
     */
    public static final int STATUS_CLOSED = 0;

    /**
     * The status of an open transaction.
     */
    public static final int STATUS_OPEN = 1;

    /**
     * The status of a prepared transaction.
     */
    public static final int STATUS_PREPARED = 2;

    /**
     * The status of a transaction that is being committed, but possibly not
     * yet finished. A transactions can go into this state when the store is
     * closed while the transaction is committing. When opening a store,
     * such transactions should be committed.
     */
    public static final int STATUS_COMMITTING = 3;

    /**
     * The transaction store.
     */
    final TransactionStore store;

    /**
     * The transaction id.
     */
    //final int transactionId;

    int transactionId;

    /**
     * The log id of the last entry in the undo log map.
     */
    long logId;

    private int status;

    private String name;

    private Transaction t;

    LocalTransaction(TransactionStore store, int transactionId, int status, String name, long logId) {
        this.store = store;
        this.transactionId = transactionId;
        this.status = status;
        this.name = name;
        this.logId = logId;
    }

    public void setTransaction(Transaction t) {
        transactionId = (int) t.getTransactionId();
        this.t = t;
    }

    public int getId() {
        return transactionId;
    }

    public int getStatus() {
        return status;
    }

    void setStatus(int status) {
        this.status = status;
    }

    public void setName(String name) {
        checkNotClosed();
        this.name = name;
        store.storeTransaction(this);
    }

    public String getName() {
        return name;
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
        store.log(this, logId, mapId, key, oldValue);
        // only increment the log id if logging was successful
        logId++;
    }

    /**
     * Remove the last log entry.
     */
    void logUndo() {
        store.logUndo(this, --logId);
    }

    /**
     * Open a data map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @return the transaction map
     */
    public <K, V> TransactionMap<K, V> openMap(String name) {
        return openMap(name, null, null);
    }

    /**
     * Open the map to store the data.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param keyType the key data type
     * @param valueType the value data type
     * @return the transaction map
     */
    public <K, V> TransactionMap<K, V> openMap(String name, DataType keyType, DataType valueType) {
        checkNotClosed();
        MVMap<K, VersionedValue> map = store.openMap(name, keyType, valueType);
        int mapId = map.getId();
        return new TransactionMap<K, V>(this, map, mapId);
    }

    /**
     * Open the transactional version of the given map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param map the base map
     * @return the transactional map
     */
    public <K, V> TransactionMap<K, V> openMap(MVMap<K, VersionedValue> map) {
        checkNotClosed();
        int mapId = map.getId();
        return new TransactionMap<K, V>(this, map, mapId);
    }

    /**
     * Prepare the transaction. Afterwards, the transaction can only be
     * committed or rolled back.
     */
    public void prepare() {
        checkNotClosed();
        status = STATUS_PREPARED;
        store.storeTransaction(this);
    }

    /**
     * Commit the transaction. Afterwards, this transaction is closed.
     */
    @Override
    public void commit() {
        if (session.isLocal())
            commit0();
        else
            commit(null);
    }

    private void commit0() {
        checkNotClosed();
        store.commit(this, logId);
        if (t != null)
            t.commit();
    }

    /**
     * Roll back to the given savepoint. This is only allowed if the
     * transaction is open.
     *
     * @param savepointId the savepoint id
     */
    public void rollbackToSavepoint(long savepointId) {
        checkNotClosed();
        store.rollbackTo(this, logId, savepointId);
        logId = savepointId;
    }

    /**
     * Roll the transaction back. Afterwards, this transaction is closed.
     */
    @Override
    public void rollback() {
        try {
            checkNotClosed();
            store.rollbackTo(this, logId, 0);
            store.endTransaction(this);
        } finally {
            endTransaction();
        }
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
    public Iterator<Change> getChanges(long savepointId) {
        return store.getChanges(this, logId, savepointId);
    }

    /**
     * Check whether this transaction is open or prepared.
     */
    void checkNotClosed() {
        if (status == STATUS_CLOSED) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "Transaction is closed");
        }
    }

    /**
     * Remove the map.
     *
     * @param map the map
     */
    public <K, V> void removeMap(TransactionMap<K, V> map) {
        store.removeMap(map);
    }

    @Override
    public String toString() {
        return "" + transactionId;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public long getCommitTimestamp() {
        return transactionId;
    }

    private boolean autoCommit;

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    private String transactionName;

    //协调者或参与者自身的本地事务名
    private StringBuilder localTransactionNamesBuilder;
    //如果本事务是协调者中的事务，那么在此字段中存放其他参与者的本地事务名
    private final ConcurrentSkipListSet<String> participantLocalTransactionNames = new ConcurrentSkipListSet<String>();

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
            for (String name : localTransactionNames.split(","))
                participantLocalTransactionNames.add(name.trim());
        }
    }

    @Override
    public String getLocalTransactionNames() {
        if (transactionName == null)
            transactionName = TransactionManager.getHostAndPort() + ":" + transactionId;
        StringBuilder buff = new StringBuilder(transactionName);

        if (!participantLocalTransactionNames.isEmpty()) {
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
    public void rollbackToSavepoint(String name) {
    }

    public String getAllLocalTransactionNames() {
        getLocalTransactionNames();
        return localTransactionNamesBuilder.toString();
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        try {
            if (allLocalTransactionNames == null)
                allLocalTransactionNames = getAllLocalTransactionNames();
            List<Future<Void>> futures = null;
            if (!isAutoCommit() && session.getFrontendSessionCache().size() > 0)
                futures = parallelCommitOrRollback(allLocalTransactionNames);

            commit0();
            store.commitTransactionStatusTable(this, allLocalTransactionNames);
            if (futures != null)
                waitFutures(futures);
        } finally {
            endTransaction();
        }
    }

    private void endTransaction() {
        if (!session.getFrontendSessionCache().isEmpty()) {
            for (FrontendSession fs : session.getFrontendSessionCache().values()) {
                fs.setTransaction(null);
                FrontendSessionPool.release(fs);
            }

            session.getFrontendSessionCache().clear();
        }

        if (!session.isRoot())
            session.setAutoCommit(true);
    }

    @Override
    public void log(Object obj) {
    }

    Session session;

    public void setSession(Session session) {
        this.session = session;
    }

    private List<Future<Void>> parallelCommitOrRollback(final String allLocalTransactionNames) {
        int size = session.getFrontendSessionCache().size();
        List<Future<Void>> futures = New.arrayList(size);
        for (final FrontendSession fs : session.getFrontendSessionCache().values()) {
            futures.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (allLocalTransactionNames != null)
                        fs.commitTransaction(allLocalTransactionNames);
                    else
                        fs.rollbackTransaction();
                    return null;
                }
            }));
        }
        return futures;
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
}