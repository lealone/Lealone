/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.client.result.ClientResult;
import org.lealone.client.result.RowCountDeterminedClientResult;
import org.lealone.client.result.RowCountUndeterminedClientResult;
import org.lealone.client.session.ClientSession;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.CommandParameter;
import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.net.TransferInputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.batch.BatchStatementUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdateAck;
import org.lealone.server.protocol.dt.DTransactionQuery;
import org.lealone.server.protocol.dt.DTransactionQueryAck;
import org.lealone.server.protocol.dt.DTransactionUpdate;
import org.lealone.server.protocol.dt.DTransactionUpdateAck;
import org.lealone.server.protocol.replication.ReplicationHandleReplicaConflict;
import org.lealone.server.protocol.replication.ReplicationUpdate;
import org.lealone.server.protocol.replication.ReplicationUpdateAck;
import org.lealone.server.protocol.statement.StatementQuery;
import org.lealone.server.protocol.statement.StatementQueryAck;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.server.protocol.statement.StatementUpdateAck;
import org.lealone.sql.DistributedSQLCommand;
import org.lealone.storage.PageKey;
import org.lealone.storage.replication.ReplicaSQLCommand;

/**
 * Represents the client-side part of a SQL statement.
 * This class is not used in embedded mode.
 * 
 * @author H2 Group
 * @author zhh
 */
public class ClientSQLCommand implements ReplicaSQLCommand, DistributedSQLCommand {

    // 通过设为null来判断是否关闭了当前命令，所以没有加上final
    protected ClientSession session;
    protected final String sql;
    protected int fetchSize;
    protected int commandId;
    protected boolean isQuery;

    public ClientSQLCommand(ClientSession session, String sql, int fetchSize) {
        this.session = session;
        this.sql = sql;
        this.fetchSize = fetchSize;
    }

    @Override
    public int getType() {
        return CLIENT_SQL_COMMAND;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public boolean isQuery() {
        return isQuery;
    }

    @Override
    public ArrayList<CommandParameter> getParameters() {
        return new ArrayList<>(0);
    }

    @Override
    public Result getMetaData() {
        return null;
    }

    @Override
    public Future<Result> executeQuery(int maxRows, boolean scrollable) {
        return query(maxRows, scrollable, null);
    }

    @Override
    public Future<Result> executeDistributedQuery(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        return query(maxRows, scrollable, pageKeys);
    }

    private Future<Result> query(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        isQuery = true;
        int fetch;
        if (scrollable) {
            fetch = Integer.MAX_VALUE;
        } else {
            fetch = fetchSize;
        }
        int resultId = session.getNextId();
        return query(maxRows, scrollable, pageKeys, fetch, resultId);

    }

    protected Future<Result> query(int maxRows, boolean scrollable, List<PageKey> pageKeys, int fetch, int resultId) {
        int packetId = commandId = session.getNextId();
        if (isDistributed()) {
            Packet packet = new DTransactionQuery(pageKeys, resultId, maxRows, fetch, scrollable, sql);
            return session.<Result, DTransactionQueryAck> send(packet, packetId, ack -> {
                addLocalTransactionNames(ack.localTransactionNames);
                return getQueryResult(ack, fetch, resultId);
            });
        } else {
            Packet packet = new StatementQuery(resultId, maxRows, fetch, scrollable, sql);
            return session.<Result, StatementQueryAck> send(packet, packetId, ack -> {
                return getQueryResult(ack, fetch, resultId);
            });
        }
    }

    protected ClientResult getQueryResult(StatementQueryAck ack, int fetch, int resultId) {
        int columnCount = ack.columnCount;
        int rowCount = ack.rowCount;
        ClientResult result = null;
        try {
            TransferInputStream in = (TransferInputStream) ack.in;
            in.setSession(session);
            if (rowCount < 0)
                result = new RowCountUndeterminedClientResult(session, in, resultId, columnCount, fetch);
            else
                result = new RowCountDeterminedClientResult(session, in, resultId, columnCount, rowCount, fetch);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        return result;
    }

    protected boolean isDistributed() {
        return session.getParentTransaction() != null && !session.getParentTransaction().isAutoCommit();
    }

    protected void addLocalTransactionNames(String localTransactionNames) {
        session.getParentTransaction().addLocalTransactionNames(localTransactionNames);
    }

    @Override
    public Future<Integer> executeUpdate() {
        int packetId = commandId = session.getNextId();
        Packet packet = new StatementUpdate(sql);
        return session.<Integer, StatementUpdateAck> send(packet, packetId, ack -> {
            return ack.updateCount;
        });
    }

    @Override
    public Future<Integer> executeDistributedUpdate(List<PageKey> pageKeys) {
        if (isDistributed()) {
            int packetId = commandId = session.getNextId();
            Packet packet = new DTransactionUpdate(pageKeys, sql);
            return session.<Integer, DTransactionUpdateAck> send(packet, packetId, ack -> {
                addLocalTransactionNames(ack.localTransactionNames);
                return ack.updateCount;
            });
        } else {
            return executeUpdate();
        }
    }

    @Override
    public Future<ReplicationUpdateAck> executeReplicaUpdate(String replicationName) {
        int packetId = commandId = session.getNextId();
        Packet packet = new ReplicationUpdate(sql, replicationName);
        return session.<ReplicationUpdateAck, ReplicationUpdateAck> send(packet, packetId, ack -> {
            ack.setReplicaCommand(ClientSQLCommand.this);
            ack.setPacketId(packetId);
            return ack;
        });
    }

    @Override
    public void close() {
        session = null;
    }

    /**
     * Cancel this current statement.
     */
    @Override
    public void cancel() {
        session.cancelStatement(commandId);
    }

    @Override
    public String toString() {
        return sql;
    }

    @Override
    public void handleReplicaConflict(List<String> retryReplicationNames) {
        try {
            session.send(new ReplicationHandleReplicaConflict(retryReplicationNames));
        } catch (Exception e) {
            session.getTrace().error(e, "handleReplicaConflict");
        }
    }

    @Override
    public void removeAsyncCallback(int packetId) {
        session.removeAsyncCallback(packetId);
    }

    public int[] executeBatchSQLCommands(List<String> batchCommands) {
        commandId = session.getNextId();
        try {
            Future<BatchStatementUpdateAck> ack = session
                    .send(new BatchStatementUpdate(batchCommands.size(), batchCommands), commandId);
            return ack.get().results;
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }
}
