/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * SHUTDOWN DATABASE [ IMMEDIATELY ]
 */
public class ShutdownDatabase extends AdminStatement {

    private final Database db;
    private final boolean immediately;

    public ShutdownDatabase(ServerSession session, Database db, boolean immediately) {
        super(session);
        this.db = db;
        this.immediately = immediately;
    }

    @Override
    public int getType() {
        return SQLStatement.SHUTDOWN_DATABASE;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "shutdown database");
        // 如果是LealoneDatabase什么都不做
        if (LealoneDatabase.isMe(db.getName()))
            return 0;
        DbObjectLock lock = LealoneDatabase.getInstance().tryExclusiveDatabaseLock(session);
        if (lock == null)
            return -1;
        db.markClosed();
        if (immediately) {
            db.shutdownImmediately();
        } else if (db.getSessionCount() == 0) {
            db.closeIfNeeded();
        }
        return 0;
    }
}
