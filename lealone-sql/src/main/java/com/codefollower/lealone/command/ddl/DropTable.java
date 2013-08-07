/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.command.ddl;

import java.util.ArrayList;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Right;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.constraint.ConstraintReferential;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.dbobject.table.TableView;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.util.StatementBuilder;

/**
 * This class represents the statement
 * DROP TABLE
 */
public class DropTable extends SchemaCommand {

    private boolean ifExists;
    private String tableName;
    private Table table;
    private DropTable next;
    private int dropAction;

    public DropTable(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ? ConstraintReferential.RESTRICT
                : ConstraintReferential.CASCADE;
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
            table.setModified();
            Database db = session.getDatabase();
            db.lockMeta(session);
            db.removeSchemaObject(session, table);
        }
        if (next != null) {
            next.executeDrop();
        }
    }

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

    public int getType() {
        return CommandInterface.DROP_TABLE;
    }

}