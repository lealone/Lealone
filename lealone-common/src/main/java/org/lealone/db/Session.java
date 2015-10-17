/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.io.Closeable;
import java.util.ArrayList;

import org.lealone.common.message.Trace;
import org.lealone.db.value.Value;
import org.lealone.sql.BatchStatement;
import org.lealone.sql.ParsedStatement;
import org.lealone.sql.PreparedStatement;
import org.lealone.transaction.Transaction;

/**
 * A client or server session. A session represents a database connection.
 */
public interface Session extends Closeable, Transaction.Participant {

    public static final int SESSION_PREPARE = 0;
    public static final int SESSION_CLOSE = 1;
    public static final int COMMAND_EXECUTE_QUERY = 2;
    public static final int COMMAND_EXECUTE_UPDATE = 3;
    public static final int COMMAND_CLOSE = 4;
    public static final int RESULT_FETCH_ROWS = 5;
    public static final int RESULT_RESET = 6;
    public static final int RESULT_CLOSE = 7;
    // public static final int COMMAND_COMMIT = 8; //不再使用
    public static final int CHANGE_ID = 9;
    public static final int COMMAND_GET_META_DATA = 10;
    public static final int SESSION_PREPARE_READ_PARAMS = 11;
    public static final int SESSION_SET_ID = 12;
    public static final int SESSION_CANCEL_STATEMENT = 13;
    // public static final int SESSION_CHECK_KEY = 14; //不再使用
    public static final int SESSION_SET_AUTOCOMMIT = 15;
    // public static final int SESSION_UNDO_LOG_POS = 16; //不再使用
    public static final int LOB_READ = 17;

    public static final int COMMAND_EXECUTE_DISTRIBUTED_QUERY = 100;
    public static final int COMMAND_EXECUTE_DISTRIBUTED_UPDATE = 101;
    public static final int COMMAND_EXECUTE_DISTRIBUTED_COMMIT = 102;
    public static final int COMMAND_EXECUTE_DISTRIBUTED_ROLLBACK = 103;

    public static final int COMMAND_EXECUTE_DISTRIBUTED_SAVEPOINT_ADD = 104;
    public static final int COMMAND_EXECUTE_DISTRIBUTED_SAVEPOINT_ROLLBACK = 105;

    public static final int COMMAND_EXECUTE_TRANSACTION_VALIDATE = 106;

    public static final int COMMAND_EXECUTE_BATCH_UPDATE_STATEMENT = 120;
    public static final int COMMAND_EXECUTE_BATCH_UPDATE_PREPAREDSTATEMENT = 121;

    public static final int STATUS_ERROR = 0;
    public static final int STATUS_OK = 1;
    public static final int STATUS_CLOSED = 2;
    public static final int STATUS_OK_STATE_CHANGED = 3;

    ParsedStatement parseStatement(String sql);

    /**
     * Parse a command and prepare it for execution.
     *
     * @param sql the SQL statement
     * @param fetchSize the number of rows to fetch in one step
     * @return the prepared command
     */
    Command prepareCommand(String sql, int fetchSize);

    PreparedStatement prepareStatement(String sql, int fetchSize);

    BatchStatement getBatchStatement(PreparedStatement ps, ArrayList<Value[]> batchParameters);

    BatchStatement getBatchStatement(ArrayList<String> batchCommands);

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
    void close();

    /**
     * Check if close was called.
     *
     * @return if the session has been closed
     */
    boolean isClosed();

    int getModificationId();

    Transaction getTransaction();

    void setTransaction(Transaction transaction);

    void rollback();

    @Override
    void addSavepoint(String name);

    @Override
    void rollbackToSavepoint(String name);

    void setRoot(boolean isRoot);

    boolean validateTransaction(String localTransactionName);

    void commit(boolean ddl, String allLocalTransactionNames);

    Session connectEmbeddedOrServer();

    String getURL();

    void checkTransfers();
}
