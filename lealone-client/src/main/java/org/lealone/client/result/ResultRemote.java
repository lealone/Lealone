/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.client.result;

import java.io.IOException;
import java.util.ArrayList;

import org.lealone.client.FrontendSession;
import org.lealone.common.message.DbException;
import org.lealone.common.message.Trace;
import org.lealone.common.util.New;
import org.lealone.db.SysProperties;
import org.lealone.db.result.ResultInterface;
import org.lealone.db.value.Transfer;
import org.lealone.db.value.Value;

public abstract class ResultRemote implements ResultInterface {

    protected int fetchSize;
    protected FrontendSession session;
    protected Transfer transfer;
    protected int id;
    protected final ResultColumn[] columns;
    protected Value[] currentRow;
    protected final int rowCount;
    protected int rowId, rowOffset;
    protected ArrayList<Value[]> result;
    protected final Trace trace;

    public ResultRemote(FrontendSession session, Transfer transfer, int id, int columnCount, int rowCount, int fetchSize)
            throws IOException {
        this.session = session;
        trace = session.getTrace();
        this.transfer = transfer;
        this.id = id;
        this.columns = new ResultColumn[columnCount];
        this.rowCount = rowCount;
        for (int i = 0; i < columnCount; i++) {
            columns[i] = new ResultColumn(transfer);
        }
        rowId = -1;
        result = New.arrayList();
        this.fetchSize = fetchSize;
        fetchRows(false);
    }

    public abstract boolean next();

    protected abstract void fetchRows(boolean sendFetch);

    public String getAlias(int i) {
        return columns[i].alias;
    }

    public String getSchemaName(int i) {
        return columns[i].schemaName;
    }

    public String getTableName(int i) {
        return columns[i].tableName;
    }

    public String getColumnName(int i) {
        return columns[i].columnName;
    }

    public int getColumnType(int i) {
        return columns[i].columnType;
    }

    public long getColumnPrecision(int i) {
        return columns[i].precision;
    }

    public int getColumnScale(int i) {
        return columns[i].scale;
    }

    public int getDisplaySize(int i) {
        return columns[i].displaySize;
    }

    public boolean isAutoIncrement(int i) {
        return columns[i].autoIncrement;
    }

    public int getNullable(int i) {
        return columns[i].nullable;
    }

    public void reset() {
        rowId = -1;
        currentRow = null;
        if (session == null) {
            return;
        }
        synchronized (session) {
            session.checkClosed();
            try {
                session.traceOperation("RESULT_RESET", id);
                transfer.writeInt(FrontendSession.RESULT_RESET).writeInt(id).flush();
            } catch (IOException e) {
                throw DbException.convertIOException(e, null);
            }
        }
    }

    public Value[] currentRow() {
        return currentRow;
    }

    public int getRowId() {
        return rowId;
    }

    public int getVisibleColumnCount() {
        return columns.length;
    }

    public int getRowCount() {
        return rowCount;
    }

    protected void sendClose() {
        if (session == null) {
            return;
        }
        // TODO result sets: no reset possible for larger remote result sets
        try {
            synchronized (session) {
                session.traceOperation("RESULT_CLOSE", id);
                transfer.writeInt(FrontendSession.RESULT_CLOSE).writeInt(id);
            }
        } catch (IOException e) {
            trace.error(e, "close");
        } finally {
            transfer = null;
            session = null;
        }
    }

    protected void sendFetch(int fetchSize) throws IOException {
        session.traceOperation("RESULT_FETCH_ROWS", id);
        transfer.writeInt(FrontendSession.RESULT_FETCH_ROWS).writeInt(id).writeInt(fetchSize);
        session.done(transfer);
    }

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
                transfer.writeInt(FrontendSession.CHANGE_ID).writeInt(id).writeInt(newId);
                id = newId;
                // TODO remote result set: very old result sets may be
                // already removed on the server (theoretically) - how to
                // solve this?
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    public String toString() {
        return "columns: " + columns.length + " rows: " + rowCount + " pos: " + rowId;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public boolean needToClose() {
        return true;
    }

    protected void fetchRowsThrowException() throws IOException {
        int available = transfer.available();
        if (transfer.readInt() == FrontendSession.STATUS_ERROR)
            session.parseError(transfer);

        throw DbException.throwInternalError("fetchRows: the available bytes was " + available);
    }
}
