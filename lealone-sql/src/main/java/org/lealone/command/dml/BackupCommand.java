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

    public int update() {
        String name = fileNameExpr.getValue(session).getString();
        session.getUser().checkAdmin();
        backupTo(name);
        return 0;
    }

    private void backupTo(String fileName) {
        session.getDatabase().backupTo(fileName);
    }

    public boolean isTransactional() {
        return true;
    }

    /**
     * Fix the file name, replacing backslash with slash.
     *
     * @param f the file name
     * @return the corrected file name
     */
    public static String correctFileName(String f) {
        f = f.replace('\\', '/');
        if (f.startsWith("/")) {
            f = f.substring(1);
        }
        return f;
    }

    public boolean needRecompile() {
        return false;
    }

    public ResultInterface queryMeta() {
        return null;
    }

    public int getType() {
        return CommandInterface.BACKUP;
    }

}
