/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * BACKUP
 */
public class Backup extends ManipulationStatement {

    private Expression fileNameExpr;

    public Backup(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.BACKUP;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setFileName(Expression fileName) {
        this.fileNameExpr = fileName;
    }

    @Override
    public int update() {
        String fileName = fileNameExpr.getValue(session).getString();
        session.getUser().checkAdmin();
        session.getDatabase().backupTo(fileName);
        return 0;
    }

}
