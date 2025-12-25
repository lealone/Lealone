/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.common.trace.Trace;
import com.lealone.common.trace.TraceModuleType;
import com.lealone.common.trace.TraceObjectType;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.DataHandler;
import com.lealone.db.RunMode;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.AsyncTask;
import com.lealone.db.async.Future;
import com.lealone.db.command.SQLCommand;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.AckPacketHandler;
import com.lealone.server.protocol.Packet;

public class DelegatedSession implements Session {

    protected Session session;

    public DelegatedSession() {
    }

    public DelegatedSession(Session session) {
        setSession(session);
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public Session getSession() {
        return session;
    }

    @Override
    public int getId() {
        return session.getId();
    }

    @Override
    public SQLCommand createSQLCommand(String sql, int fetchSize, boolean prepared) {
        return session.createSQLCommand(sql, fetchSize, prepared);
    }

    @Override
    public boolean isAutoCommit() {
        return session.isAutoCommit();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        session.setAutoCommit(autoCommit);
    }

    @Override
    public void cancel() {
        session.cancel();
    }

    @Override
    public void cancelStatement(int statementId) {
        session.cancelStatement(statementId);
    }

    @Override
    public void close() {
        session.close();
    }

    @Override
    public boolean isClosed() {
        return session.isClosed();
    }

    @Override
    public void checkClosed() {
        session.checkClosed();
    }

    @Override
    public void setInvalid(boolean v) {
        session.setInvalid(v);
    }

    @Override
    public boolean isInvalid() {
        return session.isInvalid();
    }

    @Override
    public boolean isValid() {
        return session.isValid();
    }

    @Override
    public void setTargetNodes(String targetNodes) {
        session.setTargetNodes(targetNodes);
    }

    @Override
    public String getTargetNodes() {
        return session.getTargetNodes();
    }

    @Override
    public void setRunMode(RunMode runMode) {
        session.setRunMode(runMode);
    }

    @Override
    public RunMode getRunMode() {
        return session.getRunMode();
    }

    @Override
    public Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType) {
        return session.getTrace(traceModuleType, traceObjectType);
    }

    @Override
    public Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType,
            int traceObjectId) {
        return session.getTrace(traceModuleType, traceObjectType, traceObjectId);
    }

    @Override
    public DataHandler getDataHandler() {
        return session.getDataHandler();
    }

    @Override
    public void setNetworkTimeout(int milliseconds) {
        session.setNetworkTimeout(milliseconds);
    }

    @Override
    public int getNetworkTimeout() {
        return session.getNetworkTimeout();
    }

    @Override
    public ConnectionInfo getConnectionInfo() {
        return session.getConnectionInfo();
    }

    @Override
    public <R, P extends AckPacket> Future<R> send(Packet packet,
            AckPacketHandler<R, P> ackPacketHandler) {
        return session.send(packet, ackPacketHandler);
    }

    @Override
    public <R, P extends AckPacket> Future<R> send(Packet packet, int packetId,
            AckPacketHandler<R, P> ackPacketHandler) {
        return session.send(packet, packetId, ackPacketHandler);
    }

    @Override
    public <T> AsyncCallback<T> createCallback() {
        return session.createCallback();
    }

    @Override
    public boolean isBio() {
        return session.isBio();
    }

    @Override
    public <T> void execute(AsyncCallback<T> ac, AsyncTask task) {
        session.execute(ac, task);
    }

    @Override
    public Scheduler getScheduler() {
        return session.getScheduler();
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        session.setScheduler(scheduler);
    }

    @Override
    public void setSessionInfo(SessionInfo si) {
        session.setSessionInfo(si);
    }

    @Override
    public SessionInfo getSessionInfo() {
        return session.getSessionInfo();
    }
}
