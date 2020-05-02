/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.LockTable;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableType;
import org.lealone.db.table.TableView;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.dml.Query;
import org.lealone.sql.expression.Parameter;

/**
 * This class represents the statement
 * CREATE VIEW
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateView extends SchemaStatement {

    private String viewName;
    private boolean ifNotExists;
    private Query select;
    private String selectSQL;
    private String[] columnNames;
    private String comment;
    private boolean orReplace;
    private boolean force;

    public CreateView(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_VIEW;
    }

    public void setViewName(String name) {
        viewName = name;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setSelect(Query select) {
        this.select = select;
    }

    public void setSelectSQL(String selectSQL) {
        this.selectSQL = selectSQL;
    }

    public void setColumnNames(String[] cols) {
        this.columnNames = cols;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setOrReplace(boolean orReplace) {
        this.orReplace = orReplace;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    @Override
    public int update() {
        LockTable lockTable = schema.tryExclusiveLock(DbObjectType.TABLE_OR_VIEW, session);
        if (lockTable == null)
            return -1;

        Database db = session.getDatabase();
        TableView view = null;
        Table old = getSchema().findTableOrView(session, viewName);
        if (old != null) {
            if (ifNotExists) {
                return 0;
            }
            if (!orReplace || old.getTableType() != TableType.VIEW) {
                throw DbException.get(ErrorCode.VIEW_ALREADY_EXISTS_1, viewName);
            }
            view = (TableView) old;
        }
        int id = getObjectId();
        String querySQL;
        if (select == null) {
            querySQL = selectSQL;
        } else {
            ArrayList<Parameter> params = select.getParameters();
            if (params != null && params.size() > 0) {
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, "parameters in views");
            }
            querySQL = select.getPlanSQL();
        }
        ServerSession sysSession = db.getSystemSession();
        try {
            if (view == null) {
                Schema schema = session.getDatabase().getSchema(session, session.getCurrentSchemaName());
                sysSession.setCurrentSchema(schema);
                view = new TableView(getSchema(), id, viewName, querySQL, null, columnNames, sysSession, false);
            } else {
                view.replace(querySQL, columnNames, sysSession, false, force);
            }
        } finally {
            sysSession.setCurrentSchema(db.getSchema(session, Constants.SCHEMA_MAIN));
        }
        if (comment != null) {
            view.setComment(comment);
        }
        if (old == null) {
            getSchema().add(session, view, lockTable);
        } else {
            db.updateMeta(session, view);
        }
        return 0;
    }
}
