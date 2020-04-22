/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        switch (type) {
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
