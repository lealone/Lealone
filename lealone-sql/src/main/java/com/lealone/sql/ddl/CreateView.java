/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.Constants;
import com.lealone.db.Database;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.db.table.TableType;
import com.lealone.db.table.TableView;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.query.Query;

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
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.TABLE_OR_VIEW, session);
        if (lock == null)
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
                view = new TableView(getSchema(), id, viewName, querySQL, null, columnNames, sysSession,
                        false);
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
            getSchema().add(session, view, lock);
        } else {
            db.updateMeta(session, view);
        }
        return 0;
    }
}
