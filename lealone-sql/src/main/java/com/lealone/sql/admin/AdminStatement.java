/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.admin;

import com.lealone.db.session.ServerSession;
import com.lealone.sql.StatementBase;

public abstract class AdminStatement extends StatementBase {

    public AdminStatement(ServerSession session) {
        super(session);
    }

    @Override
    public boolean needRecompile() {
        return false;
    }
}
