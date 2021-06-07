/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.auth.Right;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;

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
        DbObjectLock lock = tryAlterTable(table);
        if (lock == null)
            return -1;

        session.getUser().checkRight(table, Right.ALL);
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
