/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DbObjectType;
import com.lealone.db.auth.Right;
import com.lealone.db.schema.Schema;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.TableView;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ALTER VIEW
 * 
 * @author H2 Group
 * @author zhh
 */
public class AlterView extends SchemaStatement {

    private TableView view;

    public AlterView(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_VIEW;
    }

    public void setView(TableView view) {
        this.view = view;
    }

    @Override
    public int update() {
        session.getUser().checkRight(view, Right.ALL);
        if (schema.tryExclusiveLock(DbObjectType.TABLE_OR_VIEW, session) == null)
            return -1;

        DbException e = view.recompile(session, false);
        if (e != null) {
            throw e;
        }
        return 0;
    }
}
