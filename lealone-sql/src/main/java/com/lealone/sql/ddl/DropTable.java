/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StatementBuilder;
import com.lealone.db.Database;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.constraint.ConstraintReferential;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.db.table.TableView;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP TABLE
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropTable extends SchemaStatement {

    private String tableName;
    private boolean ifExists;
    private int dropAction;
    private Table table;
    private DropTable next;

    public DropTable(ServerSession session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ? ConstraintReferential.RESTRICT
                : ConstraintReferential.CASCADE;
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_TABLE;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
        if (next != null) {
            next.setIfExists(b);
        }
    }

    public void setDropAction(int dropAction) {
        this.dropAction = dropAction;
        if (next != null) {
            next.setDropAction(dropAction);
        }
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

    private boolean prepareDrop() {
        table = schema.findTableOrView(session, tableName);
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
            if (!table.tryExclusiveLock(session))
                return false;
        }
        if (next != null) {
            return next.prepareDrop();
        }
        return true;
    }

    private void executeDrop(DbObjectLock lock) {
        // need to get the table again, because it may be dropped already
        // meanwhile (dependent object, or same object)
        table = schema.findTableOrView(session, tableName);
        if (table != null) {
            int id = table.getId();
            table.setModified();
            schema.remove(session, table, lock);
            Database db = session.getDatabase();
            db.getTableAlterHistory().deleteRecords(id);
        }
        if (next != null) {
            next.executeDrop(lock);
        }
    }

    @Override
    public int update() {
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.TABLE_OR_VIEW, session);
        if (lock == null)
            return -1;

        if (!prepareDrop())
            return -1;
        executeDrop(lock);
        return 0;
    }
}
