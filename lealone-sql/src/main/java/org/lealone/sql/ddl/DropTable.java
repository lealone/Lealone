/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.ArrayList;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Right;
import org.lealone.db.constraint.ConstraintReferential;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableView;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP TABLE
 */
public class DropTable extends SchemaStatement {

    private boolean ifExists;
    private String tableName;
    private Table table;
    private DropTable next;
    private int dropAction;

    public DropTable(ServerSession session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ? ConstraintReferential.RESTRICT
                : ConstraintReferential.CASCADE;
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_TABLE;
    }

    /**
     * Chain another drop table statement to this statement.
     *
     * @param drop the statement to add
     */
    public void addNextDropTable(DropTable drop) {
        if (next == null) {
            next = drop;
        } else {
            next.addNextDropTable(drop);
        }
    }

    public void setIfExists(boolean b) {
        ifExists = b;
        if (next != null) {
            next.setIfExists(b);
        }
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private void prepareDrop() {
        table = getSchema().findTableOrView(session, tableName);
        if (table == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
            }
        } else {
            session.getUser().checkRight(table, Right.ALL);
            if (!table.canDrop()) {
                throw DbException.get(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
            }
            if (dropAction == ConstraintReferential.RESTRICT) {
                ArrayList<TableView> views = table.getViews();
                if (views != null && views.size() > 0) {
                    StatementBuilder buff = new StatementBuilder();
                    for (TableView v : views) {
                        buff.appendExceptFirst(", ");
                        buff.append(v.getName());
                    }
                    throw DbException.get(ErrorCode.CANNOT_DROP_2, tableName, buff.toString());
                }
            }
            table.lock(session, true, true);
        }
        if (next != null) {
            next.prepareDrop();
        }
    }

    private void executeDrop() {
        // need to get the table again, because it may be dropped already
        // meanwhile (dependent object, or same object)
        table = getSchema().findTableOrView(session, tableName);

        if (table != null) {
            int id = table.getId();
            table.setModified();
            Database db = session.getDatabase();
            db.lockMeta(session);
            db.removeSchemaObject(session, table);
            db.deleteTableAlterHistoryRecord(id);
        }
        if (next != null) {
            next.executeDrop();
        }
    }

    @Override
    public int update() {
        session.commit(true);
        prepareDrop();
        executeDrop();
        return 0;
    }

    public void setDropAction(int dropAction) {
        this.dropAction = dropAction;
        if (next != null) {
            next.setDropAction(dropAction);
        }
    }

}