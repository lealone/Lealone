/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.db.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * Represents an empty statement or a statement that has no effect.
 */
public class NoOperation extends ManipulateStatement {

    public NoOperation(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.NO_OPERATION;
    }

    @Override
    public boolean isQuery() {
        return false;
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
