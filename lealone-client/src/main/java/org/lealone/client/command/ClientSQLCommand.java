/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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
import org.lealone.server.protocol.dt.DTransactionParameters;
import org.lealone.server.protocol.dt.DTransactionQuery;
import org.lealone.server.protocol.dt.DTransactionQueryAck;
import org.lealone.server.protocol.dt.DTransactionReplicationUpdate;
import org.lealone.server.protocol.dt.DTransactionReplicationUpdateAck;
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
import org.lealone.storage.replication.ReplicaSQLCommand;

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
    public Future<Result> executeDistributedQuery(int maxRows, boolean scrollable, DTransactionParameters parameters) {
        return query(maxRows, scrollable, parameters);
    }

    private Future<Result> query(int maxRows, boolean scrollable, DTransactionParameters parameters) {
        isQuery = true;
        int fetch;
        if (scrollable) {
            fetch = Integer.MAX_VALUE;
        } else {
            fetch = fetchSize;
        }
        int resultId = session.getNextId();
        return query(maxRows, scrollable, fetch, resultId, parameters);

    }

    protected Future<Result> query(int maxRows, boolean scrollable, int fetch, int resultId,
            DTransactionParameters parameters) {
        int packetId = commandId = session.getNextId();
        if (parameters != null) {
            Packet packet = new DTransactionQuery(resultId, maxRows, fetch, scrollable, sql, parameters);
            return session.<Result, DTransactionQueryAck> send(packet, packetId, ack -> {
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

    @Override
    public Future<Integer> executeUpdate() {
        int packetId = commandId = session.getNextId();
        Packet packet = new StatementUpdate(sql);
        return session.<Integer, StatementUpdateAck> send(packet, packetId, ack -> {
            return ack.updateCount;
        });
    }

    @Override
    public Future<Integer> executeDistributedUpdate(DTransactionParameters parameters) {
        int packetId = commandId = session.getNextId();
        Packet packet = new DTransactionUpdate(sql, parameters);
        return session.<Integer, DTransactionUpdateAck> send(packet, packetId, ack -> {
            return ack.updateCount;
        });
    }

    @Override
    public Future<ReplicationUpdateAck> executeReplicaUpdate(String replicationName,
            DTransactionParameters parameters) {
        int packetId = commandId = session.getNextId();
        if (parameters != null) {
            Packet packet = new DTransactionReplicationUpdate(sql, replicationName, parameters);
            return session.<ReplicationUpdateAck, DTransactionReplicationUpdateAck> send(packet, packetId, ack -> {
                ack.setReplicaCommand(ClientSQLCommand.this);
                ack.setPacketId(packetId);
                return ack;
            });
        } else {
            Packet packet = new ReplicationUpdate(sql, replicationName);
            return session.<ReplicationUpdateAck, ReplicationUpdateAck> send(packet, packetId, ack -> {
                ack.setReplicaCommand(ClientSQLCommand.this);
                ack.setPacketId(packetId);
                return ack;
            });
        }
    }

    @Override
    public void close() {
        session = null;
    }

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
