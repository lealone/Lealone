/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.mysql.dbobject.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.table.Column;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.mysql.dbobject.table.TableLink;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.value.DataType;
import org.lealone.value.Value;

/**
 * The cursor implementation for the linked index.
 */
public class LinkedCursor implements Cursor {

    private final TableLink tableLink;
    private final PreparedStatement prep;
    private final String sql;
    private final Session session;
    private final ResultSet rs;
    private Row current;

    LinkedCursor(TableLink tableLink, ResultSet rs, Session session, String sql, PreparedStatement prep) {
        this.session = session;
        this.tableLink = tableLink;
        this.rs = rs;
        this.sql = sql;
        this.prep = prep;
    }

    public Row get() {
        return current;
    }

    public SearchRow getSearchRow() {
        return current;
    }

    public boolean next() {
        try {
            boolean result = rs.next();
            if (!result) {
                rs.close();
                tableLink.reusePreparedStatement(prep, sql);
                current = null;
                return false;
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        current = tableLink.getTemplateRow();
        for (int i = 0; i < current.getColumnCount(); i++) {
            Column col = tableLink.getColumn(i);
            Value v = DataType.readValue(session, rs, i + 1, col.getType());
            current.setValue(i, v);
        }
        return true;
    }

    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
