/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.db.CommandInterface;
import org.lealone.db.Session;
import org.lealone.db.result.ResultInterface;
import org.lealone.sql.Prepared;

/**
 * Represents an empty statement or a statement that has no effect.
 */
public class NoOperation extends Prepared {

    public NoOperation(Session session) {
        super(session);
    }

    @Override
    public int update() {
        return 0;
    }

    @Override
    public boolean isQuery() {
        return false;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.NO_OPERATION;
    }

}
