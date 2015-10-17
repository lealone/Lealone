/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.common.message.DbException;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.result.Result;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;

/**
 * Represents a transactional statement.
 */
public class TransactionStatement extends StatementBase {
    public static final String INTERNAL_SAVEPOINT = "_INTERNAL_SAVEPOINT_";

    private final int type;
    private String savepointName;
    private String transactionName;

    public TransactionStatement(ServerSession session, int type) {
        super(session);
        this.type = type;
    }

    public void setSavepointName(String name) {
        if (INTERNAL_SAVEPOINT.equals(name))
            throw DbException.getUnsupportedException("Savepoint name cannot use " + INTERNAL_SAVEPOINT);
        this.savepointName = name;
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
            session.commit(false);
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
        case SQLStatement.PREPARE_COMMIT:
            session.prepareCommit(transactionName);
            break;
        case SQLStatement.COMMIT_TRANSACTION:
            session.getUser().checkAdmin();
            session.setPreparedTransaction(transactionName, true);
            break;
        case SQLStatement.ROLLBACK_TRANSACTION:
            session.getUser().checkAdmin();
            session.setPreparedTransaction(transactionName, false);
            break;
        case SQLStatement.SHUTDOWN_IMMEDIATELY:
            session.getUser().checkAdmin();
            session.getDatabase().shutdownImmediately();
            break;
        case SQLStatement.SHUTDOWN:
        case SQLStatement.SHUTDOWN_COMPACT:
        case SQLStatement.SHUTDOWN_DEFRAG: {
            session.getUser().checkAdmin();
            session.commit(false);
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
                if (db.isMultiThreaded()) {
                    synchronized (s) {
                        s.rollback();
                    }
                } else {
                    // if not multi-threaded, the session could already own
                    // the lock, which would result in a deadlock
                    // the other session can not concurrently do anything
                    // because the current session has locked the database
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

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setTransactionName(String string) {
        this.transactionName = string;
    }

    @Override
    public Result queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

}
