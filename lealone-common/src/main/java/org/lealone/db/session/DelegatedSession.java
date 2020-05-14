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
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.async.Future;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.AckPacketHandler;
import org.lealone.server.protocol.Packet;
import org.lealone.sql.ParsedSQLStatement;
import org.lealone.sql.PreparedSQLStatement;
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

    @Override
    public int getId() {
        return session.getId();
    }

    public void setSession(Session session) {
        this.session = session;
    }

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

    @Override
    public SQLCommand createSQLCommand(String sql, int fetchSize) {
        return session.createSQLCommand(sql, fetchSize);
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
    public ParsedSQLStatement parseStatement(String sql) {
        return session.parseStatement(sql);
    }

    @Override
    public PreparedSQLStatement prepareStatement(String sql, int fetchSize) {
        return session.prepareStatement(sql, fetchSize);
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
    public void cancel() {
        session.cancel();
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
    public int getModificationId() {
        return session.getModificationId();
    }

    @Override
    public Transaction getTransaction() {
        return session.getTransaction();
    }

    @Override
    public Transaction getTransaction(PreparedSQLStatement statement) {
        return session.getTransaction(statement);
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
    public void rollback() {
        session.rollback();
    }

    @Override
    public void setRoot(boolean isRoot) {
        session.setRoot(isRoot);
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        session.commit(allLocalTransactionNames);
    }

    @Override
    public String getURL() {
        return session.getURL();
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
    public ConnectionInfo getConnectionInfo() {
        return session.getConnectionInfo();
    }

    @Override
    public boolean isLocal() {
        return session.isLocal();
    }

    @Override
    public boolean isShardingMode() {
        return session.isShardingMode();
    }

    @Override
    public int getNextId() {
        return session.getNextId();
    }

    @Override
    public void asyncCommit(Runnable asyncTask) {
        session.asyncCommit(asyncTask);
    }

    @Override
    public void asyncCommitComplete() {
        session.asyncCommitComplete();
    }

    @Override
    public SessionStatus getStatus() {
        return session.getStatus();
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
    public long getLastRowKey() {
        return session.getLastRowKey();
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
    public boolean isRunModeChanged() {
        return session.isRunModeChanged();
    }

    @Override
    public String getNewTargetNodes() {
        return session.getNewTargetNodes();
    }

    @Override
    public void runModeChanged(String newTargetNodes) {
        session.runModeChanged(newTargetNodes);
    }

    @Override
    public void reconnectIfNeeded() {
        session.reconnectIfNeeded();
    }

    @Override
    public IDatabase getDatabase() {
        return session.getDatabase();
    }

    @Override
    public Session getNestedSession(String hostAndPort, boolean remote) {
        return session.getNestedSession(hostAndPort, remote);
    }

    @Override
    public int getNetworkTimeout() {
        return session.getNetworkTimeout();
    }

    @Override
    public void cancelStatement(int statementId) {
        session.cancelStatement(statementId);
    }

    @Override
    public String getLocalHostAndPort() {
        return session.getLocalHostAndPort();
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
}
