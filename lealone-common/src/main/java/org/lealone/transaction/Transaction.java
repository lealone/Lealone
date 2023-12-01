/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import java.sql.Connection;
import java.util.Map;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionStatus;
import org.lealone.storage.Storage;
import org.lealone.storage.type.StorageDataType;

public interface Transaction {

    public static final int IL_READ_UNCOMMITTED = Connection.TRANSACTION_READ_UNCOMMITTED;

    public static final int IL_READ_COMMITTED = Connection.TRANSACTION_READ_COMMITTED;

    public static final int IL_REPEATABLE_READ = Connection.TRANSACTION_REPEATABLE_READ;

    public static final int IL_SERIALIZABLE = Connection.TRANSACTION_SERIALIZABLE;

    public static final int OPERATION_COMPLETE = 1;

    public static final int OPERATION_NEED_RETRY = 2;

    public static final int OPERATION_NEED_WAIT = 3;

    String getTransactionName();

    boolean isClosed();

    boolean isWaiting();

    int getIsolationLevel();

    default boolean isRepeatableRead() {
        return getIsolationLevel() >= Transaction.IL_REPEATABLE_READ;
    }

    long getTransactionId();

    void onSynced();

    boolean isAutoCommit();

    void setAutoCommit(boolean autoCommit);

    boolean isLocal();

    void setSession(Session session);

    Session getSession();

    Scheduler getScheduler();

    void setScheduler(Scheduler scheduler);

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

    default void addLobTask(Runnable lobTask) {
    }

    default void asyncCommit() {
        asyncCommit(null);
    }

    void asyncCommit(Runnable asyncTask);

    void asyncCommitComplete();

    void commit();

    void rollback();

    void rollbackToSavepoint(String name);

    void rollbackToSavepoint(int savepointId);

    int addWaitingTransaction(Object key, Session session, AsyncHandler<SessionStatus> asyncHandler);

}
