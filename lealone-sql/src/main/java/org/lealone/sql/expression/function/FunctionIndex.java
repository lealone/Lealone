/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.function;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.IndexBase;
import org.lealone.db.index.IndexType;
import org.lealone.db.result.Result;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.IndexColumn;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;

/**
 * An index for a function that returns a result set. This index can only scan
 * through all rows, search is not supported.
 */
public class FunctionIndex extends IndexBase {

    private final FunctionTable functionTable;

    public FunctionIndex(FunctionTable functionTable, IndexColumn[] columns) {
        super(functionTable, 0, null, IndexType.createNonUnique(), columns);
        this.functionTable = functionTable;
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        if (functionTable.isBufferResultSetToLocalTemp()) {
            return new FunctionCursor(functionTable.getResult(session));
        }
        return new FunctionCursorResultSet(session, functionTable.getResultSet(session));
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
    public long getRowCount(ServerSession session) {
        return functionTable.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return functionTable.getRowCountApproximation();
    }

    @Override
    public String getPlanSQL() {
        return "function";
    }

    @Override
    public boolean canScan() {
        return false;
    }

    /**
     * A cursor for a function that returns a result.
     */
    private static class FunctionCursor implements Cursor {

        private final Result result;
        private Value[] values;
        private Row row;

        FunctionCursor(Result result) {
            this.result = result;
        }

        @Override
        public Row get() {
            if (values == null) {
                return null;
            }
            if (row == null) {
                row = new Row(values, 1);
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            row = null;
            if (result != null && result.next()) {
                values = result.currentRow();
            } else {
                values = null;
            }
            return values != null;
        }
    }

    /**
     * A cursor for a function that returns a JDBC result set.
     */
    private static class FunctionCursorResultSet implements Cursor {

        private final ServerSession session;
        private final ResultSet result;
        private final ResultSetMetaData meta;
        private Value[] values;
        private Row row;

        FunctionCursorResultSet(ServerSession session, ResultSet result) {
            this.session = session;
            this.result = result;
            try {
                this.meta = result.getMetaData();
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }

        @Override
        public Row get() {
            if (values == null) {
                return null;
            }
            if (row == null) {
                row = new Row(values, 1);
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            row = null;
            try {
                if (result != null && result.next()) {
                    int columnCount = meta.getColumnCount();
                    values = new Value[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        int type = DataType.getValueTypeFromResultSet(meta, i + 1);
                        values[i] = DataType.readValue(session, result, i + 1, type);
                    }
                } else {
                    values = null;
                }
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            return values != null;
        }
    }
}
