/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.client.result;

import java.io.IOException;
import java.util.ArrayList;

import com.lealone.client.jdbc.JdbcAsyncCallback;
import com.lealone.client.session.ClientSession;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.Utils;
import com.lealone.db.SysProperties;
import com.lealone.db.result.Result;
import com.lealone.db.value.Value;
import com.lealone.net.TransferInputStream;
import com.lealone.server.protocol.result.ResultChangeId;
import com.lealone.server.protocol.result.ResultClose;
import com.lealone.server.protocol.result.ResultFetchRows;
import com.lealone.server.protocol.result.ResultFetchRowsAck;
import com.lealone.server.protocol.result.ResultReset;

/**
 * The client side part of a result set that is kept on the server.
 * In many cases, the complete data is kept on the client side,
 * but for large results only a subset is in-memory.
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class ClientResult implements Result {

    protected int fetchSize;
    protected ClientSession session;
    protected TransferInputStream in;
    protected int resultId; // 如果为负数，表示后端没有缓存任何东西
    protected final ClientResultColumn[] columns;
    protected Value[] currentRow;
    protected final int rowCount;
    protected int rowId, rowOffset;
    protected ArrayList<Value[]> result;

    public ClientResult(ClientSession session, TransferInputStream in, int resultId, int columnCount,
            int rowCount, int fetchSize) throws IOException {
        this.session = session;
        this.in = in;
        this.resultId = resultId;
        this.columns = new ClientResultColumn[columnCount];
        this.rowCount = rowCount;
        for (int i = 0; i < columnCount; i++) {
            columns[i] = new ClientResultColumn(in);
        }
        rowId = -1;
        result = Utils.newSmallArrayList();
        this.fetchSize = fetchSize;
        fetchRows(false);
    }

    @Override
    public abstract boolean next();

    protected abstract void fetchRows(boolean sendFetch);

    @Override
    public String getAlias(int i) {
        return columns[i].alias;
    }

    @Override
    public String getSchemaName(int i) {
        return columns[i].schemaName;
    }

    @Override
    public String getTableName(int i) {
        return columns[i].tableName;
    }

    @Override
    public String getColumnName(int i) {
        return columns[i].columnName;
    }

    @Override
    public int getColumnType(int i) {
        return columns[i].columnType;
    }

    @Override
    public long getColumnPrecision(int i) {
        return columns[i].precision;
    }

    @Override
    public int getColumnScale(int i) {
        return columns[i].scale;
    }

    @Override
    public int getDisplaySize(int i) {
        return columns[i].displaySize;
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return columns[i].autoIncrement;
    }

    @Override
    public int getNullable(int i) {
        return columns[i].nullable;
    }

    @Override
    public void reset() {
        rowId = -1;
        currentRow = null;
        if (session == null) {
            return;
        }
        if (resultId > 0) {
            session.checkClosed();
            try {
                session.send(new ResultReset(resultId));
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    @Override
    public Value[] currentRow() {
        return currentRow;
    }

    @Override
    public int getRowId() {
        return rowId;
    }

    @Override
    public int getVisibleColumnCount() {
        return columns.length;
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    public int getCurrentRowCount() {
        return result.size();
    }

    // 调度线程和外部线程都会调用
    protected void sendClose() {
        if (session == null) {
            return;
        }
        try {
            if (resultId > 0) {
                if (session != null) {
                    session.send(new ResultClose(resultId));
                    session = null;
                }
            } else {
                session = null;
            }
        } catch (Exception e) {
            session.getTrace().error(e, "close");
        }
    }

    protected TransferInputStream sendFetch(int fetchSize) throws IOException {
        // 释放buffer
        if (in.isLazyRead())
            in.closeForce();
        JdbcAsyncCallback<TransferInputStream> ac = JdbcAsyncCallback.create(session);
        session.<ResultFetchRowsAck> send(new ResultFetchRows(resultId, fetchSize)).onComplete(ar -> {
            if (ar.isSucceeded()) {
                TransferInputStream in = (TransferInputStream) ar.getResult().in;
                ac.setAsyncResult(in);
            } else {
                ac.setAsyncResult(ar.getCause());
            }
        });
        return ac.get();
    }

    @Override
    public void close() {
        result = null;
        sendClose();
    }

    protected void remapIfOld() {
        if (session == null) {
            return;
        }
        try {
            if (resultId > 0
                    && resultId <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS / 2) {
                // object is too old - we need to map it to a new id
                int newId = session.getNextId();
                session.send(new ResultChangeId(resultId, newId)); // 不需要响应
                resultId = newId;
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String toString() {
        return "columns: " + columns.length + " rows: " + rowCount + " pos: " + rowId;
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
    public boolean needToClose() {
        return true;
    }
}
