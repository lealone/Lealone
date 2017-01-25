/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

import java.sql.SQLException;

import org.lealone.api.DatabaseEventListener;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.db.result.SearchRow;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueString;
import org.lealone.sql.PreparedStatement;

/**
 * A record in the system table of the database.
 * It contains the SQL statement to create the database object.
 * 
 * @author H2 Group
 * @author zhh
 */
public class MetaRecord implements Comparable<MetaRecord> {

    private final int id;
    private final DbObjectType objectType;
    private final String sql;

    public MetaRecord(SearchRow r) {
        id = r.getValue(0).getInt();
        objectType = DbObjectType.TYPES[r.getValue(1).getInt()];
        sql = r.getValue(2).getString();
    }

    public MetaRecord(DbObject obj) {
        id = obj.getId();
        objectType = obj.getType();
        sql = obj.getCreateSQL();
    }

    public int getId() {
        return id;
    }

    public void setRecord(SearchRow r) {
        r.setValue(0, ValueInt.get(id));
        r.setValue(1, ValueInt.get(objectType.value));
        r.setValue(2, ValueString.get(sql));
    }

    /**
     * Execute the meta data statement.
     *
     * @param db the database
     * @param systemSession the system session
     * @param listener the database event listener
     */
    public void execute(Database db, ServerSession systemSession, DatabaseEventListener listener) {
        try {
            PreparedStatement command = systemSession.prepareStatementLocal(sql);
            command.setObjectId(id);
            command.executeUpdate();
        } catch (DbException e) {
            e = e.addSQL(sql);
            SQLException s = e.getSQLException();
            db.getTrace(Trace.DATABASE).error(s, sql);
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
