/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.dml;

import java.sql.Date;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.Database;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * BACKUP
 */
public class Backup extends ManipulationStatement {

    private String fileName;
    private String lastDate;

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

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setLastDate(String lastDate) {
        this.lastDate = lastDate;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        if (!db.isPersistent()) {
            throw DbException.get(ErrorCode.DATABASE_IS_NOT_PERSISTENT);
        }
        Long ld = lastDate != null ? Date.valueOf(lastDate).getTime() : null;
        db.backupTo(fileName, ld);
        return 0;
    }
}
