/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.dml;

import com.lealone.db.session.ServerSession;
import com.lealone.sql.StatementBase;

public abstract class ManipulationStatement extends StatementBase {

    public ManipulationStatement(ServerSession session) {
        super(session);
    }
}
