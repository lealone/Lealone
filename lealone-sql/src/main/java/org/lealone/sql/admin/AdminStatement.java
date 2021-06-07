/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.StatementBase;

public abstract class AdminStatement extends StatementBase {

    public AdminStatement(ServerSession session) {
        super(session);
    }

    @Override
    public boolean needRecompile() {
        return false;
    }
}
