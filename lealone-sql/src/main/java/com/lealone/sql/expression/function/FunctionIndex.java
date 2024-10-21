/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression.function;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.index.Cursor;
import com.lealone.db.index.IndexBase;
import com.lealone.db.index.IndexColumn;
import com.lealone.db.index.IndexType;
import com.lealone.db.result.Result;
import com.lealone.db.result.Row;
import com.lealone.db.result.SearchRow;
import com.lealone.db.result.SortOrder;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;

/**
 * An index for a function that returns a result set. This index can only scan
 * through all rows, search is not supported.
 * 
 * @author H2 Group
 * @author zhh
 */
public class FunctionIndex extends IndexBase {

    private final FunctionTable functionTable;

    public FunctionIndex(FunctionTable functionTable, IndexColumn[] columns) {
        super(functionTable, 0, null, IndexType.createNonUnique(), columns);
        this.functionTable = functionTable;
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        if (functionTable.isBufferResultSetToLocalTemp())
            return new FunctionResultCursor(functionTable.getResult(session));
        else
            return new FunctionResultSetCursor(session, functionTable.getResultSet(session));
    }

    @Override
    public double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        if (masks != null) {
            throw DbException.getUnsupportedException("ALIAS");
        }
        long expectedRows;
        if (functionTable.canGetRowCount()) {
            expectedRows = functionTable.getRowCountApproximation();
        } else {
            expectedRows = database.getSettings().estimatedFunctionTableRows;
        }
        return expectedRows * 10;
    }

    @Override
    public String getPlanSQL() {
        return "function";
    }

    @Override
    public boolean canScan() {
        return false;
    }

    private static abstract class FunctionCursor implements Cursor {

        protected Row row;

        @Override
        public Row get() {
            return row;
        }
    }

    /**
     * A cursor for a function that returns a result.
     */
    private static class FunctionResultCursor extends FunctionCursor {

        private final Result result;

        FunctionResultCursor(Result result) {
            this.result = result;
        }

        @Override
        public boolean next() {
            if (result != null && result.next()) {
                row = new Row(result.currentRow());
            } else {
                row = null;
            }
            return row != null;
        }
    }

    /**
     * A cursor for a function that returns a JDBC result set.
     */
    private static class FunctionResultSetCursor extends FunctionCursor {

        private final ServerSession session;
        private final ResultSet result;
        private final ResultSetMetaData meta;

        FunctionResultSetCursor(ServerSession session, ResultSet result) {
            this.session = session;
            this.result = result;
            try {
                this.meta = result.getMetaData();
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }

        @Override
        public boolean next() {
            try {
                if (result != null && result.next()) {
                    int columnCount = meta.getColumnCount();
                    Value[] values = new Value[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        int type = DataType.getValueTypeFromResultSet(meta, i + 1);
                        values[i] = DataType.readValue(session, result, i + 1, type);
                    }
                    row = new Row(values);
                } else {
                    row = null;
                }
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            return row != null;
        }
    }
}
