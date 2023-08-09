/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.session;

import java.io.Closeable;

import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.trace.TraceObjectType;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Constants;
import org.lealone.db.DataHandler;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.page.IPage;

/**
 * A client or server session. A session represents a database connection.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface Session extends Closeable {

    public static final int STATUS_OK = 1000;
    public static final int STATUS_CLOSED = 1001;
    public static final int STATUS_ERROR = 1002;

    int getId();

    SQLCommand createSQLCommand(String sql, int fetchSize);

    /**
     * Parse a command and prepare it for execution.
     *
     * @param sql the SQL statement
     * @param fetchSize the number of rows to fetch in one step
     * @return the prepared command
     */
    SQLCommand prepareSQLCommand(String sql, int fetchSize);

    public default SessionStatus getStatus() {
        return SessionStatus.TRANSACTION_NOT_START;
    }

    public default void setStatus(SessionStatus sessionStatus) {
    }

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
     * Cancel the current or next command (called when closing a connection).
     */
    void cancel();

    void cancelStatement(int statementId);

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

    void checkClosed();

    /**
     * Get the trace object
     * 
     * @param traceModuleType the module type
     * @param traceObjectType the trace object type
     * @return the trace object
     */
    Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType);

    /**
     * Get the trace object
     * 
     * @param traceModuleType the module type
     * @param traceObjectType the trace object type 
     * @param traceObjectId the trace object id
     * @return the trace object
     */
    Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType, int traceObjectId);

    /**
     * Get the data handler object.
     *
     * @return the data handler
     */
    DataHandler getDataHandler();

    void setNetworkTimeout(int milliseconds);

    int getNetworkTimeout();

    ConnectionInfo getConnectionInfo();

    default void setLobMacSalt(byte[] lobMacSalt) {
    }

    default byte[] getLobMacSalt() {
        return null;
    }

    // 以后协议修改了再使用版本号区分
    default void setProtocolVersion(int version) {
    }

    default int getProtocolVersion() {
        return Constants.TCP_PROTOCOL_VERSION_CURRENT;
    }

    default int getLockTimeout() {
        return Integer.MAX_VALUE;
    }

    default boolean isQueryCommand() {
        return false;
    }

    default boolean isForUpdate() {
        return false;
    }

    default boolean isUndoLogEnabled() {
        return true;
    }

    default void addDirtyPage(IPage page) {
    }

    default void markDirtyPages() {
    }

    default long getCurrentTid() {
        return 0;
    }
}
