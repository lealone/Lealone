/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.io.Closeable;

import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.trace.TraceObjectType;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.server.protocol.Packet;
import org.lealone.sql.ParsedSQLStatement;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMap;
import org.lealone.storage.replication.ReplicaSQLCommand;
import org.lealone.storage.replication.ReplicaStorageCommand;
import org.lealone.transaction.Transaction;

/**
 * A client or server session. A session represents a database connection.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface Session extends Closeable, Transaction.Participant {

    // 命令值会包含在协议包中不能随便改动，不同类型的命令值之间有意设置了间隔，用于后续加新命令

    public static final int SESSION_INIT = 0;

    public static final int COMMAND_QUERY = 40;
    public static final int COMMAND_UPDATE = 41;

    public static final int COMMAND_PREPARE = 50;
    public static final int COMMAND_PREPARE_READ_PARAMS = 51;
    public static final int COMMAND_PREPARED_QUERY = 52;
    public static final int COMMAND_PREPARED_UPDATE = 53;

    public static final int COMMAND_REPLICATION_UPDATE = 80;
    public static final int COMMAND_REPLICATION_PREPARED_UPDATE = 81;

    public static final int COMMAND_DISTRIBUTED_TRANSACTION_QUERY = 100;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY = 101;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_UPDATE = 102;
    public static final int COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE = 103;

    public static final int COMMAND_P2P_MESSAGE = 300;

    public static final int STATUS_OK = 1000;
    public static final int STATUS_CLOSED = 1001;
    public static final int STATUS_ERROR = 1002;
    public static final int STATUS_RUN_MODE_CHANGED = 1003;

    SQLCommand createSQLCommand(String sql, int fetchSize);

    ReplicaSQLCommand createReplicaSQLCommand(String sql, int fetchSize);

    StorageCommand createStorageCommand();

    ReplicaStorageCommand createReplicaStorageCommand();

    /**
     * Parse a command and prepare it for execution.
     *
     * @param sql the SQL statement
     * @param fetchSize the number of rows to fetch in one step
     * @return the prepared command
     */
    SQLCommand prepareSQLCommand(String sql, int fetchSize);

    ReplicaSQLCommand prepareReplicaSQLCommand(String sql, int fetchSize);

    ParsedSQLStatement parseStatement(String sql);

    PreparedSQLStatement prepareStatement(String sql, int fetchSize);

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

    void checkClosed();

    int getModificationId();

    Transaction getTransaction();

    Transaction getTransaction(PreparedSQLStatement statement);

    Transaction getParentTransaction();

    void setParentTransaction(Transaction transaction);

    void rollback();

    void setRoot(boolean isRoot);

    void commit(String allLocalTransactionNames);

    void asyncCommit(Runnable asyncTask);

    void asyncCommitComplete();

    default Session connect() {
        return connect(true);
    }

    Session connect(boolean allowRedirect);

    default void connectAsync(AsyncHandler<AsyncResult<Session>> asyncHandler) {
        connectAsync(true, asyncHandler);
    }

    void connectAsync(boolean allowRedirect, AsyncHandler<AsyncResult<Session>> asyncHandler);

    String getURL();

    String getReplicationName();

    void setReplicationName(String replicationName);

    ConnectionInfo getConnectionInfo();

    boolean isLocal();

    boolean isShardingMode();

    StorageMap<Object, Object> getStorageMap(String mapName);

    int getNextId();

    SessionStatus getStatus();

    void setInvalid(boolean v);

    boolean isInvalid();

    boolean isValid();

    void setTargetNodes(String targetNodes);

    String getTargetNodes();

    void setRunMode(RunMode runMode);

    RunMode getRunMode();

    long getLastRowKey();

    default void setLobMacSalt(byte[] lobMacSalt) {
    }

    default byte[] getLobMacSalt() {
        return null;
    }

    int getSessionId();

    boolean isRunModeChanged();

    String getNewTargetNodes();

    void runModeChanged(String newTargetNodes);

    default void reconnectIfNeeded() {
    }

    default IDatabase getDatabase() {
        return null;
    }

    default Session getNestedSession(String hostAndPort, boolean remote) {
        return null;
    }

    default String getUserName() {
        return null;
    }

    // 以后协议修改了再使用版本号区分
    default void setProtocolVersion(int version) {
    }

    default int getProtocolVersion() {
        return Constants.TCP_PROTOCOL_VERSION_CURRENT;
    }

    int getNetworkTimeout();

    void cancelStatement(int statementId);

    default int getLockTimeout() {
        return Integer.MAX_VALUE;
    }

    String getLocalHostAndPort();

    default <T> void sendAsync(Packet packet, String hostAndPort, AsyncHandler<T> handler) {
        sendAsync(packet, handler);
    }

    default <T> void sendAsync(Packet packet) {
        sendAsync(packet, null);
    }

    default <T> void sendAsync(Packet packet, AsyncHandler<T> handler) {
    }

    default <T> void sendAsync(Packet packet, int packetId, AsyncHandler<T> handler) {
    }

    default <T extends Packet> T sendSync(Packet packet) {
        return null;
    }

    default <T extends Packet> T sendSync(Packet packet, String hostAndPort) {
        return sendSync(packet);
    }

    default <T extends Packet> T sendSync(Packet packet, int packetId) {
        return null;
    }
}
