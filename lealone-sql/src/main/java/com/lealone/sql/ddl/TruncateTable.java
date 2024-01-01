/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.sql.SQLStatement;

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
        session.getUser().checkRight(table, Right.DELETE);
        if (!table.canTruncate()) {
            throw DbException.get(ErrorCode.CANNOT_TRUNCATE_1, table.getSQL());
        }
        if (!table.tryExclusiveLock(session))
            return -1;

        table.truncate(session);
        return 0;
    }
}
