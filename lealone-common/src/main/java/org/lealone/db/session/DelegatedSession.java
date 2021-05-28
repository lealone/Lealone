/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.db.session;

import org.lealone.common.trace.Trace;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.common.trace.TraceObjectType;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.DataHandler;
import org.lealone.db.RunMode;
import org.lealone.db.async.Future;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.AckPacketHandler;
import org.lealone.server.protocol.Packet;
import org.lealone.sql.DistributedSQLCommand;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.replication.ReplicaSQLCommand;
import org.lealone.storage.replication.ReplicaStorageCommand;
import org.lealone.transaction.Transaction;

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

    @Override
    public int getId() {
        return session.getId();
    }

    @Override
    public SQLCommand createSQLCommand(String sql, int fetchSize) {
        return session.createSQLCommand(sql, fetchSize);
    }

    @Override
    public DistributedSQLCommand createDistributedSQLCommand(String sql, int fetchSize) {
        return session.createDistributedSQLCommand(sql, fetchSize);
    }

    @Override
    public ReplicaSQLCommand createReplicaSQLCommand(String sql, int fetchSize) {
        return session.createReplicaSQLCommand(sql, fetchSize);
    }

    @Override
    public StorageCommand createStorageCommand() {
        return session.createStorageCommand();
    }

    @Override
    public ReplicaStorageCommand createReplicaStorageCommand() {
        return session.createReplicaStorageCommand();
    }

    @Override
    public SQLCommand prepareSQLCommand(String sql, int fetchSize) {
        return session.prepareSQLCommand(sql, fetchSize);
    }

    @Override
    public ReplicaSQLCommand prepareReplicaSQLCommand(String sql, int fetchSize) {
        return session.prepareReplicaSQLCommand(sql, fetchSize);
    }

    @Override
    public String getReplicationName() {
        return session.getReplicationName();
    }

    @Override
    public void setReplicationName(String replicationName) {
        session.setReplicationName(replicationName);
    }

    @Override
    public SessionStatus getStatus() {
        return session.getStatus();
    }

    @Override
    public void setStatus(SessionStatus sessionStatus) {
        session.setStatus(sessionStatus);
    }

    @Override
    public void setFinalResult(boolean isFinalResult) {
        session.setFinalResult(isFinalResult);
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
    public Transaction getParentTransaction() {
        return session.getParentTransaction();
    }

    @Override
    public void setParentTransaction(Transaction transaction) {
        session.setParentTransaction(transaction);
    }

    @Override
    public void asyncCommitComplete() {
        session.asyncCommitComplete();
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
    public boolean isRunModeChanged() {
        return session.isRunModeChanged();
    }

    @Override
    public void runModeChanged(String newTargetNodes) {
        session.runModeChanged(newTargetNodes);
    }

    @Override
    public Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType) {
        return session.getTrace(traceModuleType, traceObjectType);
    }

    @Override
    public Trace getTrace(TraceModuleType traceModuleType, TraceObjectType traceObjectType, int traceObjectId) {
        return session.getTrace(traceModuleType, traceObjectType, traceObjectId);
    }

    @Override
    public DataHandler getDataHandler() {
        return session.getDataHandler();
    }

    @Override
    public String getLocalHostAndPort() {
        return session.getLocalHostAndPort();
    }

    @Override
    public String getURL() {
        return session.getURL();
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
    public void reconnectIfNeeded() {
        session.reconnectIfNeeded();
    }

    @Override
    public void setLobMacSalt(byte[] lobMacSalt) {
        session.setLobMacSalt(lobMacSalt);
    }

    @Override
    public byte[] getLobMacSalt() {
        return session.getLobMacSalt();
    }

    @Override
    public <R, P extends AckPacket> Future<R> send(Packet packet, AckPacketHandler<R, P> ackPacketHandler) {
        return session.send(packet, ackPacketHandler);
    }

    @Override
    public <R, P extends AckPacket> Future<R> send(Packet packet, int packetId,
            AckPacketHandler<R, P> ackPacketHandler) {
        return session.send(packet, packetId, ackPacketHandler);
    }

    // 以下是Transaction.Participant的API

    @Override
    public void addSavepoint(String name) {
        session.addSavepoint(name);
    }

    @Override
    public void rollbackToSavepoint(String name) {
        session.rollbackToSavepoint(name);
    }

    @Override
    public void commitTransaction(String localTransactionName) {
        session.commitTransaction(localTransactionName);
    }

    @Override
    public void rollbackTransaction() {
        session.rollbackTransaction();
    }
}
