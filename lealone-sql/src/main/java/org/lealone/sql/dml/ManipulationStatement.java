/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.dml;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.StatementBase;

public abstract class ManipulationStatement extends StatementBase {

    public ManipulationStatement(ServerSession session) {
        super(session);
    }
}
