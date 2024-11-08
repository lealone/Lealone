/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction;

import java.sql.Connection;
import java.util.Map;

import com.lealone.db.async.AsyncHandler;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.session.InternalSession;
import com.lealone.db.session.SessionStatus;
import com.lealone.storage.Storage;
import com.lealone.storage.type.StorageDataType;

public interface Transaction {

    public static final int IL_READ_UNCOMMITTED = Connection.TRANSACTION_READ_UNCOMMITTED;

    public static final int IL_READ_COMMITTED = Connection.TRANSACTION_READ_COMMITTED;

    public static final int IL_REPEATABLE_READ = Connection.TRANSACTION_REPEATABLE_READ;

    public static final int IL_SERIALIZABLE = Connection.TRANSACTION_SERIALIZABLE;

    public static final int OPERATION_COMPLETE = 1;

    public static final int OPERATION_NEED_RETRY = 2;

    public static final int OPERATION_NEED_WAIT = 3;

    public static final int OPERATION_DATA_DUPLICATE = 4;

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

    void setSession(InternalSession session);

    InternalSession getSession();

    InternalScheduler getScheduler();

    void setScheduler(InternalScheduler scheduler);

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

    int addWaitingTransaction(Object lockedObject, InternalSession session,
            AsyncHandler<SessionStatus> asyncHandler);

    Transaction getParentTransaction();

    void setParentTransaction(Transaction parentTransaction);

    // 0：未提交，1：已提交，-1: 已回滚
    int getStatus(int savepointId);

    long getCommitTimestamp();
}
