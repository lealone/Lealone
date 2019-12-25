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
import org.lealone.db.CommandParameter;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.net.TransferInputStream;
import org.lealone.server.protocol.batch.BatchStatementUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdateAck;
import org.lealone.server.protocol.dt.DistributedTransactionQuery;
import org.lealone.server.protocol.dt.DistributedTransactionQueryAck;
import org.lealone.server.protocol.dt.DistributedTransactionUpdate;
import org.lealone.server.protocol.dt.DistributedTransactionUpdateAck;
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
    public Result executeQuery(int maxRows) {
        return query(maxRows, false, null, null);
    }

    @Override
    public Result executeQuery(int maxRows, boolean scrollable) {
        return query(maxRows, scrollable, null, null);
    }

    @Override
    public Result executeQuery(int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        return query(maxRows, scrollable, pageKeys, null);
    }

    @Override
    public void executeQueryAsync(int maxRows, boolean scrollable, AsyncHandler<AsyncResult<Result>> handler) {
        query(maxRows, scrollable, null, handler);
    }

    protected Result query(int maxRows, boolean scrollable, List<PageKey> pageKeys,
            AsyncHandler<AsyncResult<Result>> handler) {
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

            if (isDistributed()) {
                DistributedTransactionQuery packet = new DistributedTransactionQuery(pageKeys, resultId, maxRows, fetch,
                        scrollable, sql);
                if (handler != null) {
                    session.<DistributedTransactionQueryAck> sendAsync(packet, packetId, ack -> {
                        session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                        ClientResult result;
                        try {
                            result = getQueryResult(ack, fetch, resultId);
                            handler.handle(new AsyncResult<Result>(result));
                        } catch (IOException e) {
                            handler.handle(new AsyncResult<Result>(e));
                        }
                    });
                } else {
                    DistributedTransactionQueryAck ack = session.sendSync(packet, packetId);
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                    return getQueryResult(ack, fetch, resultId);
                }
            } else {
                StatementQuery packet = new StatementQuery(pageKeys, resultId, maxRows, fetch, scrollable, sql);
                if (handler != null) {
                    session.<StatementQueryAck> sendAsync(packet, packetId, ack -> {
                        ClientResult result;
                        try {
                            result = getQueryResult(ack, fetch, resultId);
                            handler.handle(new AsyncResult<Result>(result));
                        } catch (IOException e) {
                            handler.handle(new AsyncResult<Result>(e));
                        }
                    });
                } else {
                    StatementQueryAck ack = session.sendSync(packet, packetId);
                    return getQueryResult(ack, fetch, resultId);
                }
            }
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    protected ClientResult getQueryResult(StatementQueryAck ack, int fetch, int resultId) throws IOException {
        int columnCount = ack.columnCount;
        int rowCount = ack.rowCount;
        ClientResult result;
        if (rowCount < 0)
            result = new RowCountUndeterminedClientResult(session, (TransferInputStream) ack.in, resultId, columnCount,
                    fetch);
        else
            result = new RowCountDeterminedClientResult(session, (TransferInputStream) ack.in, resultId, columnCount,
                    rowCount, fetch);
        return result;
    }

    protected boolean isDistributed() {
        return session.getParentTransaction() != null && !session.getParentTransaction().isAutoCommit();
    }

    @Override
    public int executeUpdate() {
        return update(null, null, null);
    }

    @Override
    public int executeUpdate(List<PageKey> pageKeys) {
        return update(null, pageKeys, null);
    }

    @Override
    public void executeReplicaUpdateAsync(String replicationName, AsyncHandler<AsyncResult<Integer>> handler) {
        update(replicationName, null, handler);
    }

    @Override
    public void executeUpdateAsync(AsyncHandler<AsyncResult<Integer>> handler) {
        update(null, null, handler);
    }

    protected int update(String replicationName, List<PageKey> pageKeys, AsyncHandler<AsyncResult<Integer>> handler) {
        try {
            int packetId = session.getNextId();
            commandId = packetId;

            if (isDistributed()) {
                DistributedTransactionUpdate packet = new DistributedTransactionUpdate(pageKeys, sql);
                if (handler != null) {
                    session.<DistributedTransactionUpdateAck> sendAsync(packet, packetId, ack -> {
                        session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                        handler.handle(new AsyncResult<Integer>(ack.updateCount));
                    });
                } else {
                    DistributedTransactionUpdateAck ack = session.sendSync(packet, packetId);
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                    return ack.updateCount;
                }
            } else {
                StatementUpdate packet;
                if (replicationName != null)
                    packet = new ReplicationUpdate(pageKeys, sql, replicationName);
                else
                    packet = new StatementUpdate(pageKeys, sql);
                if (handler != null) {
                    session.<StatementUpdateAck> sendAsync(packet, packetId, ack -> {
                        handler.handle(new AsyncResult<Integer>(ack.updateCount));
                    });
                } else {
                    StatementUpdateAck ack = session.sendSync(packet, packetId);
                    return ack.updateCount;
                }
            }
        } catch (Exception e) {
            session.handleException(e);
        }
        return 0;
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
            session.sendAsync(new ReplicationCommit(validKey, autoCommit));
        } catch (Exception e) {
            session.getTrace().error(e, "replicationCommit");
        }
    }

    @Override
    public void replicaRollback() {
        try {
            session.sendAsync(new ReplicationRollback());
        } catch (Exception e) {
            session.getTrace().error(e, "replicationRollback");
        }
    }

    public int[] executeBatchSQLCommands(List<String> batchCommands) {
        commandId = session.getNextId();
        try {
            BatchStatementUpdateAck ack = session
                    .sendSync(new BatchStatementUpdate(batchCommands.size(), batchCommands), commandId);
            return ack.results;
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }
}
