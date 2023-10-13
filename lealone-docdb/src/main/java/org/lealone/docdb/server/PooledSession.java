/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server;

import org.lealone.db.Database;
import org.lealone.db.auth.User;
import org.lealone.db.session.ServerSession;

public class PooledSession extends ServerSession {

    private final DocDBServerConnection conn;

    public PooledSession(Database database, User user, int id, DocDBServerConnection conn) {
        super(database, user, id);
        this.conn = conn;
    }

    @Override
    public void close() {
        conn.addPooledSession(this);
    }
}
