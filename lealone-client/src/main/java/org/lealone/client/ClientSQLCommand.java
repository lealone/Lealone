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
import org.lealone.db.CommandUpdateResult;
import org.lealone.db.Session;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.net.AsyncCallback;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.sql.SQLCommand;
import org.lealone.storage.PageKey;

/**
 * Represents the client-side part of a SQL statement.
 * This class is not used in embedded mode.
 * 
 * @author H2 Group
 * @author zhh
 */
public class ClientSQLCommand implements SQLCommand {

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
            public void runInternal(TransferInputStream in) throws Exception {
                if (isDistributedQuery)
                    session.getParentTransaction().addLocalTransactionNames(in.readString());

                int columnCount = in.readInt();
                int rowCount = in.readInt();
                ClientResult result;
                if (rowCount < 0)
                    result = new RowCountUndeterminedClientResult(session, in, resultId, columnCount, fetch);
                else
                    result = new RowCountDeterminedClientResult(session, in, resultId, columnCount, rowCount, fetch);
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
        return update(null, null, null, null);
    }

    @Override
    public int executeUpdate(List<PageKey> pageKeys) {
        return update(null, null, pageKeys, null);
    }

    @Override
    public int executeUpdate(String replicationName, CommandUpdateResult commandUpdateResult) {
        return update(replicationName, commandUpdateResult, null, null);
    }

    @Override
    public void executeUpdateAsync(AsyncHandler<AsyncResult<Integer>> handler) {
        update(null, null, null, handler);
    }

    protected int update(String replicationName, CommandUpdateResult commandUpdateResult, List<PageKey> pageKeys,
            AsyncHandler<AsyncResult<Integer>> handler) {
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
            return getUpdateCount(out, packetId, isDistributedUpdate, commandUpdateResult, handler);
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
            CommandUpdateResult commandUpdateResult, AsyncHandler<AsyncResult<Integer>> handler) throws IOException {
        isQuery = false;
        AsyncCallback<Integer> ac = new AsyncCallback<Integer>() {
            @Override
            public void runInternal(TransferInputStream in) throws Exception {
                if (isDistributedUpdate)
                    session.getParentTransaction().addLocalTransactionNames(in.readString());

                int updateCount = in.readInt();
                long key = in.readLong();
                if (commandUpdateResult != null) {
                    commandUpdateResult.setUpdateCount(updateCount);
                    commandUpdateResult.addResult(ClientSQLCommand.this, key);
                }
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
    public void replicationCommit(long validKey, boolean autoCommit) {
        int packetId = session.getNextId();
        session.traceOperation("COMMAND_REPLICATION_COMMIT", packetId);
        TransferOutputStream out = session.newOut();
        try {
            out.writeRequestHeader(packetId, Session.COMMAND_REPLICATION_COMMIT);
            out.writeLong(validKey).writeBoolean(autoCommit).flush();
        } catch (IOException e) {
            session.getTrace().error(e, "replicationCommit");
        }
    }

    @Override
    public void replicationRollback() {
        int packetId = session.getNextId();
        session.traceOperation("COMMAND_REPLICATION_ROLLBACK", packetId);
        try {
            session.newOut().writeRequestHeader(packetId, Session.COMMAND_REPLICATION_ROLLBACK).flush();
        } catch (IOException e) {
            session.getTrace().error(e, "replicationRollback");
        }
    }
}
