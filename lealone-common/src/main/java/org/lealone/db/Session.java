/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.io.Closeable;
import java.util.concurrent.Callable;

import org.lealone.common.trace.Trace;
import org.lealone.sql.ParsedStatement;
import org.lealone.sql.PreparedStatement;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;

/**
 * A client or server session. A session represents a database connection.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface Session extends Closeable, Transaction.Participant {

    // 命令值会包含在协议包中不能随便改动，不同类型的命令值之间有意设置了间隔，用于后续加新命令

    public static final int SESSION_SET_ID = 0;
    public static final int SESSION_CANCEL_STATEMENT = 1;
    public static final int SESSION_SET_AUTO_COMMIT = 2;
    public static final int SESSION_CLOSE = 3;
    public static final int SESSION_INIT = 4;

    public static final int RESULT_FETCH_ROWS = 30;
    public static final int RESULT_CHANGE_ID = 31;
    public static final int RESULT_RESET = 32;
    public static final int RESULT_CLOSE = 33;

    public static final int COMMAND_QUERY = 50;
    public static final int COMMAND_UPDATE = 51;

    public static final int COMMAND_PREPARE = 52;
    public static final int COMMAND_PREPARE_READ_PARAMS = 53;
    public static final int COMMAND_PREPARED_QUERY = 54;
    public static final int COMMAND_PREPARED_UPDATE = 55;

    public static final int COMMAND_GET_META_DATA = 56;
    public static final int COMMAND_READ_LOB = 57;
    public static final int COMMAND_CLOSE = 58;

    public static final int COMMAND_REPLICATION_UPDATE = 80;
    public static final int COMMAND_REPLICATION_PREPARED_UPDATE = 81;

    public static final int COMMAND_DISTRIBUTED_TRANSACTION_QUERY = 100;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY = 101;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_UPDATE = 102;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE = 103;

    public static final int COMMAND_DISTRIBUTED_TRANSACTION_COMMIT = 120;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK = 121;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT = 122;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT = 123;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE = 124;

    public static final int COMMAND_BATCH_STATEMENT_UPDATE = 140;
    public static final int COMMAND_BATCH_STATEMENT_PREPARED_UPDATE = 141;

    public static final int COMMAND_STORAGE_PUT = 160;
    public static final int COMMAND_STORAGE_REPLICATION_PUT = 161;
    public static final int COMMAND_STORAGE_DISTRIBUTED_PUT = 162;

    public static final int COMMAND_STORAGE_GET = 170;
    public static final int COMMAND_STORAGE_DISTRIBUTED_GET = 171;

    public static final int COMMAND_STORAGE_MOVE_LEAF_PAGE = 180;
    public static final int COMMAND_STORAGE_REMOVE_LEAF_PAGE = 181;

    public static final int STATUS_ERROR = 0;
    public static final int STATUS_OK = 1;
    public static final int STATUS_OK_STATE_CHANGED = 2;
    public static final int STATUS_CLOSED = 3;

    Command createCommand(String sql, int fetchSize);

    StorageCommand createStorageCommand();

    /**
     * Parse a command and prepare it for execution.
     *
     * @param sql the SQL statement
     * @param fetchSize the number of rows to fetch in one step
     * @return the prepared command
     */
    Command prepareCommand(String sql, int fetchSize);

    ParsedStatement parseStatement(String sql);

    PreparedStatement prepareStatement(String sql, int fetchSize);

    /**
     * Check if this session is in auto-commit mode.
     *
     * @return true if the session is in auto-commit mode
     */
    boolean isAutoCommit();

    /**
     * Set the auto-commit mode. 
     * This call doesn't commit the current transaction.
     *
     * @param autoCommit the new value
     */
    void setAutoCommit(boolean autoCommit);

    /**
     * Get the trace object
     *
     * @return the trace object
     */
    Trace getTrace();

    /**
     * Get the data handler object.
     *
     * @return the data handler
     */
    DataHandler getDataHandler();

    /**
     * Cancel the current or next command (called when closing a connection).
     */
    void cancel();

    /**
     * Roll back pending transactions and close the session.
     */
    @Override
    public void close();

    /**
     * Check if close was called.
     *
     * @return if the session has been closed
     */
    boolean isClosed();

    int getModificationId();

    Transaction getTransaction();

    boolean containsTransaction();

    void setTransaction(Transaction transaction);

    void rollback();

    void setRoot(boolean isRoot);

    boolean validateTransaction(String localTransactionName);

    void commit(boolean ddl, String allLocalTransactionNames);

    Session connectEmbeddedOrServer();

    String getURL();

    String getReplicationName();

    void setReplicationName(String replicationName);

    ConnectionInfo getConnectionInfo();

    void setLocal(boolean local);

    boolean isLocal();

    boolean isShardingMode();

    StorageMap<Object, Object> getStorageMap(String mapName);

    int getNextId();

    void setCallable(Callable<?> callable);

    Callable<?> getCallable();

    void prepareCommit(boolean ddl);

}
