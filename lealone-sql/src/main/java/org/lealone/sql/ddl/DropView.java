/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.CommandInterface;
import org.lealone.db.DbObject;
import org.lealone.db.Right;
import org.lealone.db.Session;
import org.lealone.db.constraint.ConstraintReferential;
import org.lealone.db.schema.Schema;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableView;

/**
 * This class represents the statement
 * DROP VIEW
 */
public class DropView extends SchemaCommand {

    private String viewName;
    private boolean ifExists;
    private int dropAction;

    public DropView(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ? ConstraintReferential.RESTRICT
                : ConstraintReferential.CASCADE;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setDropAction(int dropAction) {
        this.dropAction = dropAction;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    @Override
    public int update() {
        session.commit(true);
        Table view = getSchema().findTableOrView(session, viewName);
        if (view == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.VIEW_NOT_FOUND_1, viewName);
            }
        } else {
            if (!Table.VIEW.equals(view.getTableType())) {
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

            view.lock(session, true, true);
            session.getDatabase().removeSchemaObject(session, view);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_VIEW;
    }

}
