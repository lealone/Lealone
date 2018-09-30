/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * Represents a transactional statement.
 */
public class TransactionStatement extends ManipulationStatement {

    private final int type;
    private String savepointName;

    public TransactionStatement(ServerSession session, int type) {
        super(session);
        this.type = type;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setSavepointName(String name) {
        if (INTERNAL_SAVEPOINT.equals(name))
            throw DbException.getUnsupportedException("Savepoint name cannot use " + INTERNAL_SAVEPOINT);
        this.savepointName = name;
    }

    public void setTransactionName(String string) {
        // 2PC语句已经废弃
    }

    @Override
    public int update() {
        switch (type) {
        case SQLStatement.SET_AUTOCOMMIT_TRUE:
            session.setAutoCommit(true);
            break;
        case SQLStatement.SET_AUTOCOMMIT_FALSE:
            session.setAutoCommit(false);
            break;
        case SQLStatement.BEGIN:
            session.begin();
            break;
        case SQLStatement.COMMIT:
            session.commit();
            break;
        case SQLStatement.ROLLBACK:
            session.rollback();
            break;
        case SQLStatement.CHECKPOINT:
            session.getUser().checkAdmin();
            session.getDatabase().checkpoint();
            break;
        case SQLStatement.SAVEPOINT:
            session.addSavepoint(savepointName);
            break;
        case SQLStatement.ROLLBACK_TO_SAVEPOINT:
            session.rollbackToSavepoint(savepointName);
            break;
        case SQLStatement.CHECKPOINT_SYNC:
            session.getUser().checkAdmin();
            session.getDatabase().sync();
            break;
        case SQLStatement.PREPARE_COMMIT: // 2PC语句已经废弃
            break;
        case SQLStatement.COMMIT_TRANSACTION: // 2PC语句已经废弃
            break;
        case SQLStatement.ROLLBACK_TRANSACTION: // 2PC语句已经废弃
            break;
        case SQLStatement.SHUTDOWN_IMMEDIATELY:
            session.getUser().checkAdmin();
            session.getDatabase().shutdownImmediately();
            break;
        case SQLStatement.SHUTDOWN:
        case SQLStatement.SHUTDOWN_COMPACT:
        case SQLStatement.SHUTDOWN_DEFRAG: {
            session.getUser().checkAdmin();
            session.commit();
            if (type == SQLStatement.SHUTDOWN_COMPACT || type == SQLStatement.SHUTDOWN_DEFRAG) {
                session.getDatabase().setCompactMode(type);
            }
            // close the database, but don't update the persistent setting
            session.getDatabase().setCloseDelay(0);
            Database db = session.getDatabase();
            // throttle, to allow testing concurrent
            // execution of shutdown and query
            session.throttle();
            for (ServerSession s : db.getSessions(false)) {
                synchronized (s) {
                    s.rollback();
                }
                if (s != session) {
                    s.close();
                }
            }
            session.close();
            break;
        }
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

}
