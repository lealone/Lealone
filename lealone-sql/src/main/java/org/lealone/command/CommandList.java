/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command;

import java.util.ArrayList;

import org.lealone.api.ParameterInterface;
import org.lealone.engine.Session;
import org.lealone.result.ResultInterface;

/**
 * Represents a list of SQL statements.
 */
class CommandList extends Command {

    private final Command command;
    private final String remaining;

    CommandList(Session session, String sql, Command c, String remaining) {
        super(session, sql);
        this.command = c;
        this.remaining = remaining;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return command.getParameters();
    }

    private int executeRemaining() {
        Command remainingCommand = session.prepareCommand(remaining);
        if (remainingCommand.isQuery()) {
            remainingCommand.query(0);
            return 0;
        } else {
            return remainingCommand.update();
        }
    }

    @Override
    public int update() {
        int updateCount = command.executeUpdate();
        updateCount += executeRemaining();
        return updateCount;
    }

    @Override
    public ResultInterface query(int maxrows) {
        ResultInterface result = command.query(maxrows);
        executeRemaining();
        return result;
    }

    @Override
    public boolean isQuery() {
        return command.isQuery();
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return command.queryMeta();
    }

    @Override
    public int getCommandType() {
        return command.getCommandType();
    }

    @Override
    public Prepared getPrepared() {
        return command.getPrepared();
    }

}
