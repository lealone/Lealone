/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * SHUTDOWN [ IMMEDIATELY | COMPACT | DEFRAG ]
 */
public class ShutdownDatabase extends AdminStatement {

    private final int type;

    public ShutdownDatabase(ServerSession session, int type) {
        super(session);
        this.type = type;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        switch (type) {
        case SQLStatement.SHUTDOWN_IMMEDIATELY:
            db.shutdownImmediately();
            break;
        case SQLStatement.SHUTDOWN:
        case SQLStatement.SHUTDOWN_COMPACT:
        case SQLStatement.SHUTDOWN_DEFRAG: {
            session.commit();
            if (type == SQLStatement.SHUTDOWN_COMPACT || type == SQLStatement.SHUTDOWN_DEFRAG) {
                db.setCompactMode(type);
            }
            // close the database, but don't update the persistent setting
            db.setCloseDelay(0);
            // throttle, to allow testing concurrent execution of shutdown and query
            session.throttle();
            for (ServerSession s : db.getSessions(false)) {
                synchronized (s) {
                    s.rollback();
                }
                if (s != session) {
                    s.close();
                }
            }
            // 如果在这里关闭session，那就无法给客户端返回更新结果了
            // session.close();
            break;
        }
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }
}
