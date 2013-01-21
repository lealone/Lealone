/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.command.dml;

import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.command.Prepared;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.result.ResultInterface;

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
