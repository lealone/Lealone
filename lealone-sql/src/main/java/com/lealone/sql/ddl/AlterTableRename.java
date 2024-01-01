/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ALTER TABLE RENAME
 * 
 * @author H2 Group
 * @author zhh
 */
public class AlterTableRename extends SchemaStatement {

    private Table oldTable;
    private String newTableName;
    private boolean hidden;

    public AlterTableRename(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_TABLE_RENAME;
    }

    public void setOldTable(Table table) {
        oldTable = table;
    }

    public void setNewTableName(String name) {
        newTableName = name;
    }

    public void setHidden(boolean hidden) {
        this.hidden = hidden;
    }

    @Override
    public int update() {
        session.getUser().checkRight(oldTable, Right.ALL);
        DbObjectLock lock = tryAlterTable(oldTable);
        if (lock == null)
            return -1;

        Table t = schema.findTableOrView(session, newTableName);
        if (t != null && hidden && newTableName.equals(oldTable.getName())) {
            if (!t.isHidden()) {
                oldTable.setHidden(true);
                session.getDatabase().updateMeta(session, oldTable);
            }
            return 0;
        }
        if (t != null || newTableName.equals(oldTable.getName())) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, newTableName);
        }
        if (oldTable.isTemporary()) {
            throw DbException.getUnsupportedException("temp table");
        }
        schema.rename(session, oldTable, newTableName, lock);
        return 0;
    }
}
