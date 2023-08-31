/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.TraceModuleType;
import org.lealone.db.api.DatabaseEventListener;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueString;
import org.lealone.sql.PreparedSQLStatement;

/**
 * A record in the system table of the database.
 * It contains the SQL statement to create the database object.
 * 
 * @author H2 Group
 * @author zhh
 */
public class MetaRecord implements Comparable<MetaRecord> {

    public static Row getRow(Table metaTable, DbObject obj) {
        Row Row = metaTable.getTemplateRow();
        Row.setValue(0, ValueInt.get(obj.getId()));
        Row.setValue(1, ValueInt.get(obj.getType().value));
        Row.setValue(2, ValueString.get(obj.getCreateSQL()));
        return Row;
    }

    private final int id;
    private final DbObjectType objectType;
    private final String sql;

    public MetaRecord(SearchRow r) {
        id = r.getValue(0).getInt();
        objectType = DbObjectType.TYPES[r.getValue(1).getInt()];
        sql = r.getValue(2).getString();
    }

    public int getId() {
        return id;
    }

    /**
     * Execute the meta data statement.
     *
     * @param db the database
     * @param systemSession the system session
     * @param listener the database event listener
     */
    public void execute(Database db, ServerSession systemSession, DatabaseEventListener listener) {
        execute(db, systemSession, listener, sql, id);
    }

    public static void execute(Database db, ServerSession systemSession, DatabaseEventListener listener,
            String sql, int id) {
        try {
            PreparedSQLStatement command = systemSession.prepareStatementLocal(sql);
            // 设置好数据库对象id，这样在执行create语句创建数据库对象时就能复用上一次得到的id了
            command.setObjectId(id);
            command.executeUpdate();
        } catch (DbException e) {
            e = e.addSQL(sql);
            SQLException s = e.getSQLException();
            db.getTrace(TraceModuleType.DATABASE).error(s, sql);
            if (listener != null) {
                listener.exceptionThrown(s, sql);
                // continue startup in this case
            } else {
                throw e;
            }
        }
    }

    /**
     * Sort the list of meta records by 'create order'.
     *
     * @param other the other record
     * @return -1, 0, or 1
     */
    @Override
    public int compareTo(MetaRecord other) {
        int c1 = objectType.createOrder;
        int c2 = other.objectType.createOrder;
        if (c1 != c2) {
            return c1 - c2;
        }
        return getId() - other.getId();
    }

    @Override
    public String toString() {
        return "MetaRecord [id=" + id + ", objectType=" + objectType + ", sql=" + sql + "]";
    }
}
