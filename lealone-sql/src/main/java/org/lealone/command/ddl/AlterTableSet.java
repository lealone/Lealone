/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import org.lealone.command.CommandInterface;
import org.lealone.dbobject.Right;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

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
        switch (type) {
        case CommandInterface.ALTER_TABLE_SET_REFERENTIAL_INTEGRITY:
            table.setCheckForeignKeyConstraints(session, value, value ? checkExisting : false);
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

    public int getType() {
        return type;
    }

}
