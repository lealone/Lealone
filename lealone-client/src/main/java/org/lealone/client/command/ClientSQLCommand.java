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
import org.lealone.server.protocol.replication.ReplicationCommit;
import org.lealone.server.protocol.replication.ReplicationRollback;
import org.lealone.server.protocol.replication.ReplicationUpdate;
import org.lealone.server.protocol.statement.StatementQuery;
import org.lealone.server.protocol.statement.StatementQueryAck;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.server.protocol.statement.StatementUpdateAck;
import org.lealone.storage.PageKey;
import org.lealone.storage.replication.ReplicaSQLCommand;

/**
 * Represents the client-side part of a SQL statement.
 * This class is not used in embedded mode.
 * 
 * @author H2 Group
 * @author zhh
 */
public class ClientSQLCommand implements ReplicaSQLCommand {

    // 通过设为null来判断是否关闭了当前命令，所以没有加上final
    protected ClientSession session;
    protected final String sql;
    protected final int fetchSize;
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
    public Future<Result> executeQuery(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        return query(maxRows, scrollable, pageKeys);
    }

    protected Future<Result> query(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        isQuery = true;
        int fetch;
        if (scrollable) {
            fetch = Integer.MAX_VALUE;
        } else {
            fetch = fetchSize;
        }
        try {
            int packetId = session.getNextId();
            commandId = packetId;
            int resultId = session.getNextId();
            Packet packet;
            if (isDistributed()) {
                packet = new DTransactionQuery(pageKeys, resultId, maxRows, fetch, scrollable, sql);
                return session.<Result, DTransactionQueryAck> send(packet, packetId, ack -> {
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                    return getQueryResult(ack, fetch, resultId);
                });
            } else {
                packet = new StatementQuery(pageKeys, resultId, maxRows, fetch, scrollable, sql);
                return session.<Result, StatementQueryAck> send(packet, packetId, ack -> {
                    return getQueryResult(ack, fetch, resultId);
                });
            }
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    protected ClientResult getQueryResult(StatementQueryAck ack, int fetch, int resultId) {
        int columnCount = ack.columnCount;
        int rowCount = ack.rowCount;
        ClientResult result = null;
        try {
            if (rowCount < 0)
                result = new RowCountUndeterminedClientResult(session, (TransferInputStream) ack.in, resultId,
                        columnCount, fetch);
            else
                result = new RowCountDeterminedClientResult(session, (TransferInputStream) ack.in, resultId,
                        columnCount, rowCount, fetch);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        return result;
    }

    protected boolean isDistributed() {
        return session.getParentTransaction() != null && !session.getParentTransaction().isAutoCommit();
    }

    @Override
    public Future<Integer> executeUpdate(List<PageKey> pageKeys) {
        return update(null, pageKeys);
    }

    @Override
    public Future<Integer> executeReplicaUpdate(String replicationName) {
        return update(replicationName, null);
    }

    protected Future<Integer> update(String replicationName, List<PageKey> pageKeys) {
        int packetId = session.getNextId();
        commandId = packetId;
        Packet packet;
        if (isDistributed()) {
            packet = new DTransactionUpdate(pageKeys, sql);
            return session.<Integer, DTransactionUpdateAck> send(packet, packetId, ack -> {
                session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                return ack.updateCount;
            });
        } else {
            if (replicationName != null)
                packet = new ReplicationUpdate(pageKeys, sql, replicationName);
            else
                packet = new StatementUpdate(pageKeys, sql);
            return session.<Integer, StatementUpdateAck> send(packet, packetId, ack -> {
                return ack.updateCount;
            });
        }
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
    public int getId() {
        return commandId;
    }

    @Override
    public void replicaCommit(long validKey, boolean autoCommit) {
        try {
            session.send(new ReplicationCommit(validKey, autoCommit));
        } catch (Exception e) {
            session.getTrace().error(e, "replicationCommit");
        }
    }

    @Override
    public void replicaRollback() {
        try {
            session.send(new ReplicationRollback());
        } catch (Exception e) {
            session.getTrace().error(e, "replicationRollback");
        }
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
