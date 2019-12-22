/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.io.Closeable;
import java.nio.ByteBuffer;

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
    public static final int SESSION_CANCEL_STATEMENT = 1;
    public static final int SESSION_SET_AUTO_COMMIT = 2;
    public static final int SESSION_CLOSE = 3;

    public static final int RESULT_FETCH_ROWS = 20;
    public static final int RESULT_CHANGE_ID = 21;
    public static final int RESULT_RESET = 22;
    public static final int RESULT_CLOSE = 23;

    public static final int COMMAND_QUERY = 40;
    public static final int COMMAND_UPDATE = 41;

    public static final int COMMAND_PREPARE = 50;
    public static final int COMMAND_PREPARE_READ_PARAMS = 51;
    public static final int COMMAND_PREPARED_QUERY = 52;
    public static final int COMMAND_PREPARED_UPDATE = 53;

    public static final int COMMAND_GET_META_DATA = 70;
    public static final int COMMAND_READ_LOB = 71;
    public static final int COMMAND_CLOSE = 72;

    public static final int COMMAND_REPLICATION_UPDATE = 80;
    public static final int COMMAND_REPLICATION_PREPARED_UPDATE = 81;
    public static final int COMMAND_REPLICATION_COMMIT = 82;
    public static final int COMMAND_REPLICATION_ROLLBACK = 83;
    public static final int COMMAND_REPLICATION_CHECK_CONFLICT = 84;
    public static final int COMMAND_REPLICATION_HANDLE_CONFLICT = 85;

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
    public static final int COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_PUT = 162;
    public static final int COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_REPLICATION_PUT = 163;
    public static final int COMMAND_STORAGE_APPEND = 164;
    public static final int COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_APPEND = 165;

    public static final int COMMAND_STORAGE_GET = 170;
    public static final int COMMAND_STORAGE_DISTRIBUTED_TRANSACTION_GET = 171;

    public static final int COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE = 180;
    public static final int COMMAND_STORAGE_MOVE_LEAF_PAGE = 181;
    public static final int COMMAND_STORAGE_REMOVE_LEAF_PAGE = 182;
    public static final int COMMAND_STORAGE_REPLICATE_ROOT_PAGES = 183;
    public static final int COMMAND_STORAGE_READ_PAGE = 184;

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

    boolean validateTransaction(String localTransactionName);

    String checkReplicationConflict(String mapName, ByteBuffer key, String replicationName);

    void handleReplicationConflict(String mapName, ByteBuffer key, String replicationName);

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

    // ConnectionInfo getConnectionInfo(String newHostAndPort);

    boolean isLocal();

    boolean isShardingMode();

    StorageMap<Object, Object> getStorageMap(String mapName);

    void replicateRootPages(String dbName, ByteBuffer rootPages);

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

    void replicationCommit(long validKey, boolean autoCommit);

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

    int getNetworkTimeout();

    void cancelStatement(int statementId);

    default int getLockTimeout() {
        return Integer.MAX_VALUE;
    }

    String getLocalHostAndPort();

    default <T> void send(Packet packet, String hostAndPort, AsyncHandler<T> handler) {
        send(packet, handler);
    }

    default <T> void send(Packet packet, AsyncHandler<T> handler) {
    }

    default <T> void send(Packet packet, int packetId, AsyncHandler<T> handler) {
    }

    default <T extends Packet> T sendSync(Packet packet) {
        return null;
    }

    default <T extends Packet> T sendSync(Packet packet, int packetId) {
        return null;
    }

    // default <T> void sendMessage(Message message, String hostAndPort, AsyncCallback<T> asyncCallback) {
    // }
    //
    // default void receiveMessage(NetInputStream in, int operation) throws IOException {
    // }
}
