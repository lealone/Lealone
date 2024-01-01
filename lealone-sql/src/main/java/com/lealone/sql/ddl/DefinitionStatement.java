/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.db.session.ServerSession;
import com.lealone.sql.StatementBase;

/**
 * This class represents a definition statement, for example a CREATE or DROP.
 */
public abstract class DefinitionStatement extends StatementBase {

    /**
     * Create a new command for the given session.
     *
     * @param session the session
     */
    protected DefinitionStatement(ServerSession session) {
        super(session);
        priority = MIN_PRIORITY; // DDL语句的优先级默认最小
    }

    @Override
    public boolean isDDL() {
        return true;
    }
}
