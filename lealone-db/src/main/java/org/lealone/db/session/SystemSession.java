/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.session;

import org.lealone.db.Database;
import org.lealone.db.RunMode;
import org.lealone.db.auth.User;

public class SystemSession extends ServerSession {

    public SystemSession(Database database, User user, int id) {
        super(database, user, id);
    }

    @Override
    public RunMode getRunMode() {
        return RunMode.CLIENT_SERVER;
    }
}
