/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.Right;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * TRUNCATE TABLE
 * 
 * @author H2 Group
 * @author zhh
 */
public class TruncateTable extends SchemaStatement {

    private Table table;

    public TruncateTable(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.TRUNCATE_TABLE;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public int update() {
        synchronized (getSchema().getLock(DbObjectType.TABLE_OR_VIEW)) {
            if (!table.canTruncate()) {
                throw DbException.get(ErrorCode.CANNOT_TRUNCATE_1, table.getSQL());
            }
            session.getUser().checkRight(table, Right.DELETE);
            table.lock(session, true, true);
            table.truncate(session);
        }
        return 0;
    }

}
