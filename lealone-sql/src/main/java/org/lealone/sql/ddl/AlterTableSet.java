/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Right;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ALTER TABLE SET
 */
public class AlterTableSet extends SchemaStatement {

    private String tableName;
    private final int type;

    private final boolean value;
    private boolean checkExisting;

    public AlterTableSet(ServerSession session, Schema schema, int type, boolean value) {
        super(session, schema);
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
    public boolean isTransactional() {
        return true;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public int update() {
        Table table = getSchema().getTableOrView(session, tableName);
        session.getUser().checkRight(table, Right.ALL);
        table.lock(session, true, true);
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
