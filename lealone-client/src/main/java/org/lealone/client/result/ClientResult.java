/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.result;

import java.io.IOException;
import java.util.ArrayList;

import org.lealone.client.ClientSession;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.common.util.New;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.net.Transfer;
import org.lealone.net.VoidAsyncCallback;

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
    protected Transfer transfer;
    protected int id;
    protected final ClientResultColumn[] columns;
    protected Value[] currentRow;
    protected final int rowCount;
    protected int rowId, rowOffset;
    protected ArrayList<Value[]> result;
    protected final Trace trace;

    public ClientResult(ClientSession session, Transfer transfer, int id, int columnCount, int rowCount, int fetchSize)
            throws IOException {
        this.session = session;
        trace = session.getTrace();
        this.transfer = transfer;
        this.id = id;
        this.columns = new ClientResultColumn[columnCount];
        this.rowCount = rowCount;
        for (int i = 0; i < columnCount; i++) {
            columns[i] = new ClientResultColumn(transfer);
        }
        rowId = -1;
        result = New.arrayList();
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
        session.checkClosed();
        try {
            session.traceOperation("RESULT_RESET", id);
            transfer.writeRequestHeader(id, Session.RESULT_RESET).flush();
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
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

    protected void sendClose() {
        if (session == null) {
            return;
        }
        // TODO result sets: no reset possible for larger remote result sets
        try {
            session.traceOperation("RESULT_CLOSE", id);
            transfer.writeRequestHeader(id, Session.RESULT_CLOSE).flush();
        } catch (IOException e) {
            trace.error(e, "close");
        } finally {
            transfer = null;
            session = null;
        }
    }

    protected void sendFetch(int fetchSize) throws IOException {
        session.traceOperation("RESULT_FETCH_ROWS", id);
        transfer.writeRequestHeader(id, Session.RESULT_FETCH_ROWS).writeInt(fetchSize);

        VoidAsyncCallback ac = new VoidAsyncCallback();
        transfer.addAsyncCallback(id, ac);
        transfer.flush();
        ac.await();
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
            if (id <= session.getCurrentId() - SysProperties.SERVER_CACHED_OBJECTS / 2) {
                // object is too old - we need to map it to a new id
                int newId = session.getNextId();
                session.traceOperation("CHANGE_ID", id);
                transfer.writeRequestHeader(id, Session.RESULT_CHANGE_ID).writeInt(newId).flush();
                id = newId;
                // TODO remote result set: very old result sets may be
                // already removed on the server (theoretically) - how to
                // solve this?
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
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
