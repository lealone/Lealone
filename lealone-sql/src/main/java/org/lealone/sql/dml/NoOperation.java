/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * Represents an empty statement or a statement that has no effect.
 */
public class NoOperation extends ManipulationStatement {

    public NoOperation(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.NO_OPERATION;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public int update() {
        return 0;
    }
}
