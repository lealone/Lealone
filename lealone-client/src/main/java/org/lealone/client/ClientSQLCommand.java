/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.client.result.ClientResult;
import org.lealone.client.result.RowCountDeterminedClientResult;
import org.lealone.client.result.RowCountUndeterminedClientResult;
import org.lealone.db.CommandParameter;
import org.lealone.db.Session;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.net.NetInputStream;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.server.protocol.batch.BatchStatementUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdateAck;
import org.lealone.server.protocol.dt.DistributedTransactionUpdate;
import org.lealone.server.protocol.dt.DistributedTransactionUpdateAck;
import org.lealone.server.protocol.replication.ReplicationCommit;
import org.lealone.server.protocol.replication.ReplicationRollback;
import org.lealone.server.protocol.replication.ReplicationUpdate;
import org.lealone.server.protocol.replication.ReplicationUpdateAck;
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
        String operation;
        int packetType;
        boolean isDistributedQuery = isDistributed();
        if (isDistributedQuery) {
            operation = "COMMAND_DISTRIBUTED_TRANSACTION_QUERY";
            packetType = Session.COMMAND_DISTRIBUTED_TRANSACTION_QUERY;
        } else {
            operation = "COMMAND_QUERY";
            packetType = Session.COMMAND_QUERY;
        }
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
            TransferOutputStream out = session.newOut();
            writeQueryHeader(out, operation, packetId, packetType, resultId, maxRows, fetch, scrollable, pageKeys);
            out.writeString(sql);
            return getQueryResult(out, packetId, isDistributedQuery, fetch, resultId, handler);
        } catch (Exception e) {
            session.handleException(e);
        }
        return null;
    }

    protected boolean isDistributed() {
        return session.getParentTransaction() != null && !session.getParentTransaction().isAutoCommit();
    }

    protected void writeQueryHeader(TransferOutputStream out, String operation, int packetId, int packetType,
            int resultId, int maxRows, int fetch, boolean scrollable, List<PageKey> pageKeys) throws Exception {
        session.traceOperation(operation, packetId);
        out.writeRequestHeader(packetId, packetType);
        out.writeInt(resultId).writeInt(maxRows).writeInt(fetch).writeBoolean(scrollable);
        writePageKeys(out, pageKeys);
    }

    private static void writePageKeys(TransferOutputStream out, List<PageKey> pageKeys) throws IOException {
        if (pageKeys == null) {
            out.writeInt(0);
        } else {
            int size = pageKeys.size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                PageKey pk = pageKeys.get(i);
                out.writePageKey(pk);
            }
        }
    }

    protected Result getQueryResult(TransferOutputStream out, int packetId, boolean isDistributedQuery, int fetch,
            int resultId, AsyncHandler<AsyncResult<Result>> handler) throws IOException {
        isQuery = true;
        AsyncCallback<ClientResult> ac = new AsyncCallback<ClientResult>() {
            @Override
            public void runInternal(NetInputStream in) throws Exception {
                if (isDistributedQuery)
                    session.getParentTransaction().addLocalTransactionNames(in.readString());

                int columnCount = in.readInt();
                int rowCount = in.readInt();
                ClientResult result;
                if (rowCount < 0)
                    result = new RowCountUndeterminedClientResult(session, (TransferInputStream) in, resultId,
                            columnCount, fetch);
                else
                    result = new RowCountDeterminedClientResult(session, (TransferInputStream) in, resultId,
                            columnCount, rowCount, fetch);
                setResult(result);
                if (handler != null) {
                    AsyncResult<Result> r = new AsyncResult<>();
                    r.setResult(result);
                    handler.handle(r);
                }
            }
        };
        if (handler != null) {
            ac.setAsyncHandler(handler);
            out.flush(packetId, ac);
            return null;
        } else {
            return out.flushAndAwait(packetId, ac);
        }
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
        String operation;
        int packetType;
        boolean isDistributedUpdate = isDistributed();
        if (isDistributedUpdate) {
            operation = "COMMAND_DISTRIBUTED_TRANSACTION_UPDATE";
            packetType = Session.COMMAND_DISTRIBUTED_TRANSACTION_UPDATE;
        } else if (replicationName != null) {
            operation = "COMMAND_REPLICATION_UPDATE";
            packetType = Session.COMMAND_REPLICATION_UPDATE;
        } else {
            operation = "COMMAND_UPDATE";
            packetType = Session.COMMAND_UPDATE;
        }
        try {
            int packetId = session.getNextId();
            commandId = packetId;
            TransferOutputStream out = session.newOut();
            writeUpdateHeader(out, operation, packetId, packetType, replicationName, pageKeys);
            out.writeString(sql);
            return getUpdateCount(out, packetId, isDistributedUpdate, handler);
        } catch (Exception e) {
            session.handleException(e);
        }
        return 0;
    }

    protected void writeUpdateHeader(TransferOutputStream out, String operation, int packetId, int packetType,
            String replicationName, List<PageKey> pageKeys) throws Exception {
        session.traceOperation(operation, packetId);
        out.writeRequestHeader(packetId, packetType);
        if (replicationName != null)
            out.writeString(replicationName);
        writePageKeys(out, pageKeys);
    }

    protected int getUpdateCount(TransferOutputStream out, int packetId, boolean isDistributedUpdate,
            AsyncHandler<AsyncResult<Integer>> handler) throws IOException {
        isQuery = false;
        AsyncCallback<Integer> ac = new AsyncCallback<Integer>() {
            @Override
            public void runInternal(NetInputStream in) throws Exception {
                if (isDistributedUpdate)
                    session.getParentTransaction().addLocalTransactionNames(in.readString());

                int updateCount = in.readInt();
                setResult(updateCount);
                if (handler != null) {
                    AsyncResult<Integer> r = new AsyncResult<>();
                    r.setResult(updateCount);
                    handler.handle(r);
                }
            }
        };
        int updateCount;
        if (handler != null) {
            updateCount = -1;
            ac.setAsyncHandler(handler);
            out.flush(packetId, ac);
        } else {
            updateCount = out.flushAndAwait(packetId, ac);
        }
        return updateCount;
    }

    protected int update2(String replicationName, List<PageKey> pageKeys, AsyncHandler<AsyncResult<Integer>> handler) {
        isQuery = false;
        int packetId = session.getNextId();
        commandId = packetId;
        boolean isDistributedUpdate = isDistributed();
        if (isDistributedUpdate) {
            DistributedTransactionUpdate packet = new DistributedTransactionUpdate(pageKeys, sql);
            if (handler == null) {
                DistributedTransactionUpdateAck ack = session.sendSync(packet, packetId);
                return ack.updateCount;
            } else {
                session.<DistributedTransactionUpdateAck> sendAsync(packet, packetId, ack -> {
                    session.getParentTransaction().addLocalTransactionNames(ack.localTransactionNames);
                    handler.handle(new AsyncResult<>(ack.updateCount));
                });
            }
        } else if (replicationName != null) {
            ReplicationUpdate packet = new ReplicationUpdate(pageKeys, sql, replicationName);
            if (handler == null) {
                ReplicationUpdateAck ack = session.sendSync(packet, packetId);
                return ack.updateCount;
            } else {
                session.<ReplicationUpdateAck> sendAsync(packet, packetId, ack -> {
                    handler.handle(new AsyncResult<>(ack.updateCount));
                });
            }
        } else {
            StatementUpdate packet = new StatementUpdate(pageKeys, sql);
            if (handler == null) {
                StatementUpdateAck ack = session.sendSync(packet, packetId);
                return ack.updateCount;
            } else {
                session.<StatementUpdateAck> sendAsync(packet, packetId, ack -> {
                    handler.handle(new AsyncResult<>(ack.updateCount));
                });
            }
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
