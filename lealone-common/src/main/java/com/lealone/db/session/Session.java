/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.session;

import java.io.Closeable;

import com.lealone.common.trace.Trace;
import com.lealone.common.trace.TraceModuleType;
import com.lealone.common.trace.TraceObjectType;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.Constants;
import com.lealone.db.DataHandler;
import com.lealone.db.RunMode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.AckPacketHandler;
import com.lealone.server.protocol.Packet;
import com.lealone.sql.PreparedSQLStatement.YieldableCommand;
import com.lealone.sql.SQLCommand;
import com.lealone.storage.page.IPage;
import com.lealone.transaction.Transaction;

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

    SQLCommand createSQLCommand(String sql, int fetchSize, boolean prepared);

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

    void setInvalid(boolean v);

    boolean isInvalid();

    boolean isValid();

    void setTargetNodes(String targetNodes);

    String getTargetNodes();

    void setRunMode(RunMode runMode);

    RunMode getRunMode();

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

    default void setConnectionInfo(ConnectionInfo ci) {
    }

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

    <R, P extends AckPacket> Future<R> send(Packet packet, AckPacketHandler<R, P> ackPacketHandler);

    <R, P extends AckPacket> Future<R> send(Packet packet, int packetId,
            AckPacketHandler<R, P> ackPacketHandler);

    void setSingleThreadCallback(boolean singleThreadCallback);

    boolean isSingleThreadCallback();

    <T> AsyncCallback<T> createCallback();

    default void setLockedBy(SessionStatus sessionStatus, Transaction lockedBy, Object lockedKey) {
    }

    default Transaction getTransaction() {
        return null;
    }

    default void addLock(Object lock) {
    }

    default void addWaitingScheduler(Scheduler scheduler) {
    }

    default void wakeUpWaitingSchedulers() {
    }

    default boolean compareAndSet(SessionStatus expect, SessionStatus update) {
        return false;
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

    default void addPageReference(Object ref) {
    }

    default void addPageReference(Object oldRef, Object lRef, Object rRef) {
    }

    default boolean containsPageReference(Object ref) {
        return false;
    }

    default void addDirtyPage(IPage page) {
        addDirtyPage(null, page);
    }

    default void addDirtyPage(IPage old, IPage page) {
    }

    default void markDirtyPages() {
    }

    Scheduler getScheduler();

    void setScheduler(Scheduler scheduler);

    void setYieldableCommand(YieldableCommand yieldableCommand);

    YieldableCommand getYieldableCommand();

    YieldableCommand getYieldableCommand(boolean checkTimeout, TimeoutListener timeoutListener);

    public static interface TimeoutListener {
        void onTimeout(YieldableCommand c, Throwable e);
    }

    void init();

    default boolean isBio() {
        return false;
    }

    void setSessionInfo(SessionInfo si);

    SessionInfo getSessionInfo();
}
