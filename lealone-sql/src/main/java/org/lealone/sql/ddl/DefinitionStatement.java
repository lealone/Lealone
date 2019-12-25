/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.StatementBase;

/**
 * This class represents a non-transaction statement, for example a CREATE or DROP.
 */
public abstract class DefinitionStatement extends StatementBase {

    /**
     * Create a new command for the given session.
     *
     * @param session the session
     */
    protected DefinitionStatement(ServerSession session) {
        super(session);
    }

    @Override
    public Result getMetaData() {
        return null;
    }

    @Override
    public int getPriority() {
        priority = MIN_PRIORITY;
        return priority;
    }

    @Override
    public boolean isDDL() {
        return true;
    }
}
