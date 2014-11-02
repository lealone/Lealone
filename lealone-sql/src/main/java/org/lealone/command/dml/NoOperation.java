/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.dml;

import org.lealone.command.CommandInterface;
import org.lealone.command.Prepared;
import org.lealone.engine.Session;
import org.lealone.result.ResultInterface;

/**
 * Represents an empty statement or a statement that has no effect.
 */
public class NoOperation extends Prepared {

    public NoOperation(Session session) {
        super(session);
    }

    public int update() {
        return 0;
    }

    public boolean isQuery() {
        return false;
    }

    public boolean isTransactional() {
        return true;
    }

    public boolean needRecompile() {
        return false;
    }

    public boolean isReadOnly() {
        return true;
    }

    public ResultInterface queryMeta() {
        return null;
    }

    public int getType() {
        return CommandInterface.NO_OPERATION;
    }

}
