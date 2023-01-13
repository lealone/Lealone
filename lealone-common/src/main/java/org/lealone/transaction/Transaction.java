/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import java.sql.Connection;
import java.util.Map;

import org.lealone.db.session.Session;
import org.lealone.storage.Storage;
import org.lealone.storage.type.StorageDataType;

public interface Transaction {

    int IL_READ_UNCOMMITTED = Connection.TRANSACTION_READ_UNCOMMITTED;

    int IL_READ_COMMITTED = Connection.TRANSACTION_READ_COMMITTED;

    int IL_REPEATABLE_READ = Connection.TRANSACTION_REPEATABLE_READ;

    int IL_SERIALIZABLE = Connection.TRANSACTION_SERIALIZABLE;

    /**
     * The status of a closed transaction (committed or rolled back).
     */
    public static final int STATUS_CLOSED = 0;

    /**
     * The status of an open transaction.
     */
    public static final int STATUS_OPEN = 1;

    /**
     * The status of a transaction that is being committed, but possibly not
     * yet finished. A transaction can go into this state when the store is
     * closed while the transaction is committing. When opening a store,
     * such transactions should be committed.
     */
    public static final int STATUS_COMMITTING = 2;

    public static final int STATUS_WAITING = 3;

    public static final int OPERATION_COMPLETE = 1;

    public static final int OPERATION_NEED_RETRY = 2;

    public static final int OPERATION_NEED_WAIT = 3;

    String getTransactionName();

    int getStatus();

    void setStatus(int status);

    void setIsolationLevel(int level);

    int getIsolationLevel();

    long getTransactionId();

    boolean isAutoCommit();

    void setAutoCommit(boolean autoCommit);

    void setSession(Session session);

    Session getSession();

    void checkTimeout();

    /**
     * Open a data map.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @return the transaction map
     */
    <K, V> TransactionMap<K, V> openMap(String name, Storage storage);

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
    <K, V> TransactionMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType,
            Storage storage);

    <K, V> TransactionMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType,
            Storage storage, Map<String, String> parameters);

    void addSavepoint(String name);

    int getSavepointId();

    default void asyncCommit() {
        asyncCommit(null);
    }

    void asyncCommit(Runnable asyncTask);

    void asyncCommitComplete();

    void commit();

    void rollback();

    void rollbackToSavepoint(String name);

    void rollbackToSavepoint(int savepointId);

    int addWaitingTransaction(Object key, Transaction transaction, TransactionListener listener);

    void wakeUpWaitingTransactions();
}
