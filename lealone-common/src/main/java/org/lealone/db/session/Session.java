/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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
import org.lealone.db.RunMode;
import org.lealone.db.async.Future;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.AckPacketHandler;
import org.lealone.server.protocol.Packet;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.StorageCommand;
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

    public static final int STATUS_OK = 1000;
    public static final int STATUS_CLOSED = 1001;
    public static final int STATUS_ERROR = 1002;
    public static final int STATUS_RUN_MODE_CHANGED = 1003;
    public static final int STATUS_REPLICATING = 1004;

    int getId();

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

    String getReplicationName();

    void setReplicationName(String replicationName);

    default boolean isReplicationMode() {
        return getReplicationName() != null;
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

    Transaction getParentTransaction();

    void setParentTransaction(Transaction transaction);

    void asyncCommitComplete();

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

    void setInvalid(boolean v);

    boolean isInvalid();

    boolean isValid();

    void setTargetNodes(String targetNodes);

    String getTargetNodes();

    void setRunMode(RunMode runMode);

    RunMode getRunMode();

    boolean isRunModeChanged();

    void runModeChanged(String newTargetNodes);

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

    String getLocalHostAndPort();

    String getURL();

    int getNetworkTimeout();

    ConnectionInfo getConnectionInfo();

    default void reconnectIfNeeded() {
    }

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

    @SuppressWarnings("unchecked")
    default <P extends AckPacket> Future<P> send(Packet packet) {
        return send(packet, p -> {
            return (P) p;
        });
    }

    @SuppressWarnings("unchecked")
    default <P extends AckPacket> Future<P> send(Packet packet, int packetId) {
        return send(packet, packetId, p -> {
            return (P) p;
        });
    }

    @SuppressWarnings("unchecked")
    default <P extends AckPacket> Future<P> send(Packet packet, String hostAndPort) {
        return send(packet, hostAndPort, p -> {
            return (P) p;
        });
    }

    default <R, P extends AckPacket> Future<R> send(Packet packet, String hostAndPort,
            AckPacketHandler<R, P> ackPacketHandler) {
        return send(packet, ackPacketHandler);
    }

    <R, P extends AckPacket> Future<R> send(Packet packet, AckPacketHandler<R, P> ackPacketHandler);

    <R, P extends AckPacket> Future<R> send(Packet packet, int packetId, AckPacketHandler<R, P> ackPacketHandler);
}
