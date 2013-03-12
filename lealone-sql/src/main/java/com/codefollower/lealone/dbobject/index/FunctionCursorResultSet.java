/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.dbobject.index;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.value.DataType;
import com.codefollower.lealone.value.Value;

/**
 * A cursor for a function that returns a JDBC result set.
 */
public class FunctionCursorResultSet implements Cursor {

    private final Session session;
    private final ResultSet result;
    private final ResultSetMetaData meta;
    private Value[] values;
    private Row row;

    FunctionCursorResultSet(Session session, ResultSet result) {
        this.session = session;
        this.result = result;
        try {
            this.meta = result.getMetaData();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    public Row get() {
        if (values == null) {
            return null;
        }
        if (row == null) {
            row = new Row(values, 1);
        }
        return row;
    }

    public SearchRow getSearchRow() {
        return get();
    }

    public boolean next() {
        row = null;
        try {
            if (result != null && result.next()) {
                int columnCount = meta.getColumnCount();
                values = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    int type = DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1));
                    values[i] = DataType.readValue(session, result, i+1, type);
                }
            } else {
                values = null;
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        return values != null;
    }

    public boolean previous() {
        throw DbException.throwInternalError();
    }

}