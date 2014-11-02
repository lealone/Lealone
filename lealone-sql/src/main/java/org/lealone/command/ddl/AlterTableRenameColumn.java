/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import org.lealone.command.CommandInterface;
import org.lealone.dbobject.DbObject;
import org.lealone.dbobject.Right;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Database;
import org.lealone.engine.Session;

/**
 * This class represents the statement
 * ALTER TABLE ALTER COLUMN RENAME
 */
public class AlterTableRenameColumn extends DefineCommand {

    private Table table;
    private Column column;
    private String newName;

    public AlterTableRenameColumn(Session session) {
        super(session);
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public void setNewColumnName(String newName) {
        this.newName = newName;
    }

    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkRight(table, Right.ALL);
        table.checkSupportAlter();
        table.renameColumn(column, newName);
        table.setModified();
        db.update(session, table);
        for (DbObject child : table.getChildren()) {
            if (child.getCreateSQL() != null) {
                db.update(session, child);
            }
        }
        return 0;
    }

    public int getType() {
        return CommandInterface.ALTER_TABLE_ALTER_COLUMN_RENAME;
    }

}
