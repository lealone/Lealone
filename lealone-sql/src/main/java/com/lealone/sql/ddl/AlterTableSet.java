/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.auth.Right;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ALTER TABLE SET
 * 
 * @author H2 Group
 * @author zhh
 */
public class AlterTableSet extends SchemaStatement {

    private final Table table;
    private final int type;
    private final boolean value;
    private boolean checkExisting;

    public AlterTableSet(ServerSession session, Table table, int type, boolean value) {
        super(session, table.getSchema());
        this.table = table;
        this.type = type;
        this.value = value;
    }

    @Override
    public int getType() {
        return type;
    }

    public void setCheckExisting(boolean b) {
        this.checkExisting = b;
    }

    @Override
    public int update() {
        session.getUser().checkRight(table, Right.ALL);
        DbObjectLock lock = tryAlterTable(table);
        if (lock == null)
            return -1;

        switch (type) {
        case SQLStatement.ALTER_TABLE_SET_REFERENTIAL_INTEGRITY:
            table.setCheckForeignKeyConstraints(session, value, value ? checkExisting : false);
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }
}
