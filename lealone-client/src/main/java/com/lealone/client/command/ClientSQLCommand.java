/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.lealone.client.result.ClientResult;
import com.lealone.client.result.RowCountDeterminedClientResult;
import com.lealone.client.result.RowCountUndeterminedClientResult;
import com.lealone.client.session.ClientSession;
import com.lealone.common.exceptions.DbException;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import com.lealone.db.command.CommandParameter;
import com.lealone.db.command.SQLCommand;
import com.lealone.db.result.Result;
import com.lealone.net.TransferInputStream;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.batch.BatchStatementUpdate;
import com.lealone.server.protocol.batch.BatchStatementUpdateAck;
import com.lealone.server.protocol.statement.StatementQuery;
import com.lealone.server.protocol.statement.StatementQueryAck;
import com.lealone.server.protocol.statement.StatementUpdate;
import com.lealone.server.protocol.statement.StatementUpdateAck;

public class ClientSQLCommand implements SQLCommand {

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
    public Future<Result> getMetaData() {
        return Future.succeededFuture(null);
    }

    @Override
    public Future<Result> executeQuery(int maxRows, boolean scrollable) {
        try {
            isQuery = true;
            int fetch;
            if (scrollable) {
                fetch = Integer.MAX_VALUE;
            } else {
                fetch = fetchSize;
            }
            int resultId = session.getNextId();
            return query(maxRows, scrollable, fetch, resultId);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    protected Future<Result> query(int maxRows, boolean scrollable, int fetch, int resultId) {
        int packetId = commandId = session.getNextId();
        Packet packet = new StatementQuery(resultId, maxRows, fetch, scrollable, sql);
        return session.<Result, StatementQueryAck> send(packet, packetId, ack -> {
            return getQueryResult(ack, fetch, resultId);
        });
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
                result = new RowCountDeterminedClientResult(session, in, resultId, columnCount, rowCount,
                        fetch);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        return result;
    }

    @Override
    public Future<Integer> executeUpdate() {
        try {
            int packetId = commandId = session.getNextId();
            Packet packet = new StatementUpdate(sql);
            return session.<Integer, StatementUpdateAck> send(packet, packetId, ack -> {
                return ack.updateCount;
            });
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    @Override
    public Future<Boolean> prepare(boolean readParams) {
        throw DbException.getInternalError();
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

    public AsyncCallback<int[]> executeBatchSQLCommands(List<String> batchCommands) {
        AsyncCallback<int[]> ac = session.createCallback();
        commandId = session.getNextId();
        try {
            Future<BatchStatementUpdateAck> f = session
                    .send(new BatchStatementUpdate(batchCommands.size(), batchCommands), commandId);
            f.onComplete(ar -> {
                if (ar.isSucceeded()) {
                    ac.setAsyncResult(ar.getResult().results);
                } else {
                    ac.setAsyncResult(ar.getCause());
                }
            });
        } catch (Exception e) {
            ac.setAsyncResult(e);
        }
        return ac;
    }

    protected static <T> Future<T> failedFuture(Throwable t) {
        return Future.failedFuture(t);
    }
}
