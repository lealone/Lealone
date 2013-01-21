/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.command.ddl;

import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.dbobject.Right;
import com.codefollower.yourbase.dbobject.Schema;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.message.DbException;

/**
 * This class represents the statement
 * ALTER TABLE SET
 */
public class AlterTableSet extends SchemaCommand {

    private String tableName;
    private final int type;

    private final boolean value;
    private boolean checkExisting;

    public AlterTableSet(Session session, Schema schema, int type, boolean value) {
        super(session, schema);
        this.type = type;
        this.value = value;
    }

    public void setCheckExisting(boolean b) {
        this.checkExisting = b;
    }

    public boolean isTransactional() {
        return true;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int update() {
        Table table = getSchema().getTableOrView(session, tableName);
        session.getUser().checkRight(table, Right.ALL);
        table.lock(session, true, true);
        switch(type) {
        case CommandInterface.ALTER_TABLE_SET_REFERENTIAL_INTEGRITY:
            table.setCheckForeignKeyConstraints(session, value, value ? checkExisting : false);
            break;
        default:
            DbException.throwInternalError("type="+type);
        }
        return 0;
    }

    public int getType() {
        return type;
    }

}
