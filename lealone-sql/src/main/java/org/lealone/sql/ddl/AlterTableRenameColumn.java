/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.db.Database;
import org.lealone.db.DbObject;
import org.lealone.db.DbObjectType;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Right;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.optimizer.SingleColumnResolver;

/**
 * This class represents the statement
 * ALTER TABLE ALTER COLUMN RENAME
 * 
 * @author H2 Group
 * @author zhh
 */
public class AlterTableRenameColumn extends SchemaStatement {

    private Table table;
    private Column column;
    private String newName;

    public AlterTableRenameColumn(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_TABLE_ALTER_COLUMN_RENAME;
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

    @Override
    public int update() {
        synchronized (getSchema().getLock(DbObjectType.TABLE_OR_VIEW)) {
            Database db = session.getDatabase();
            session.getUser().checkRight(table, Right.ALL);
            table.checkSupportAlter();
            // we need to update CHECK constraint
            // since it might reference the name of the column
            Expression newCheckExpr = (Expression) column.getCheckConstraint(session, newName);
            table.renameColumn(column, newName);
            column.removeCheckConstraint();
            SingleColumnResolver resolver = new SingleColumnResolver(column);
            column.addCheckConstraint(session, newCheckExpr, resolver);
            table.setModified();
            db.updateMeta(session, table);
            for (DbObject child : table.getChildren()) {
                if (child.getCreateSQL() != null) {
                    db.updateMeta(session, child);
                }
            }
        }
        return 0;
    }

}
