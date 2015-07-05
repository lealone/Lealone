/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.dml;

import org.lealone.command.CommandInterface;
import org.lealone.command.Prepared;
import org.lealone.engine.Session;
import org.lealone.expression.Expression;
import org.lealone.result.ResultInterface;

/**
 * This class represents the statement
 * BACKUP
 */
public class BackupCommand extends Prepared {

    private Expression fileNameExpr;

    public BackupCommand(Session session) {
        super(session);
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

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.BACKUP;
    }

}
