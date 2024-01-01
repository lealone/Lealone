/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DbObject;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.constraint.ConstraintReferential;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.db.table.TableType;
import com.lealone.db.table.TableView;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP VIEW
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropView extends SchemaStatement {

    private String viewName;
    private boolean ifExists;
    private int dropAction;

    public DropView(ServerSession session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ? ConstraintReferential.RESTRICT
                : ConstraintReferential.CASCADE;
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_VIEW;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setDropAction(int dropAction) {
        this.dropAction = dropAction;
    }

    @Override
    public int update() {
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.TABLE_OR_VIEW, session);
        if (lock == null)
            return -1;

        Table view = schema.findTableOrView(session, viewName);
        if (view == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.VIEW_NOT_FOUND_1, viewName);
            }
        } else {
            if (view.getTableType() != TableType.VIEW) {
                throw DbException.get(ErrorCode.VIEW_NOT_FOUND_1, viewName);
            }
            session.getUser().checkRight(view, Right.ALL);

            if (dropAction == ConstraintReferential.RESTRICT) {
                for (DbObject child : view.getChildren()) {
                    if (child instanceof TableView) {
                        throw DbException.get(ErrorCode.CANNOT_DROP_2, viewName, child.getName());
                    }
                }
            }
            schema.remove(session, view, lock);
        }
        return 0;
    }
}
