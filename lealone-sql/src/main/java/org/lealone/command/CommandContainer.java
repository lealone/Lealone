/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command;

import java.util.ArrayList;

import org.lealone.api.DatabaseEventListener;
import org.lealone.api.ParameterInterface;
import org.lealone.engine.Session;
import org.lealone.expression.Parameter;
import org.lealone.result.ResultInterface;
import org.lealone.value.Value;
import org.lealone.value.ValueNull;

/**
 * Represents a single SQL statements.
 * It wraps a prepared statement.
 */
class CommandContainer extends Command {

    private Prepared prepared;
    private boolean readOnlyKnown;
    private boolean readOnly;

    CommandContainer(Session session, String sql, Prepared prepared) {
        super(session, sql);
        prepared.setCommand(this);
        this.prepared = prepared;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return prepared.getParameters();
    }

    @Override
    public boolean isTransactional() {
        return prepared.isTransactional();
    }

    @Override
    public boolean isQuery() {
        return prepared.isQuery();
    }

    private void recompileIfRequired() {
        if (prepared.needRecompile()) {
            // TODO test with 'always recompile'
            prepared.setModificationMetaId(0);
            String sql = prepared.getSQL();
            ArrayList<Parameter> oldParams = prepared.getParameters();
            Parser parser = session.createParser();
            prepared = parser.parse(sql);
            long mod = prepared.getModificationMetaId();
            prepared.setModificationMetaId(0);
            ArrayList<Parameter> newParams = prepared.getParameters();
            for (int i = 0, size = newParams.size(); i < size; i++) {
                Parameter old = oldParams.get(i);
                if (old.isValueSet()) {
                    Value v = old.getValue(session);
                    Parameter p = newParams.get(i);
                    p.setValue(v);
                }
            }
            prepared.prepare();
            prepared.setModificationMetaId(mod);
        }
    }

    @Override
    public int update() {
        recompileIfRequired();
        setProgress(DatabaseEventListener.STATE_STATEMENT_START);
        start();
        session.setLastScopeIdentity(ValueNull.INSTANCE);
        prepared.checkParameters();
        int updateCount = prepared.update();
        prepared.trace(startTime, updateCount);
        setProgress(DatabaseEventListener.STATE_STATEMENT_END);
        return updateCount;
    }

    @Override
    public ResultInterface query(int maxrows) {
        recompileIfRequired();
        setProgress(DatabaseEventListener.STATE_STATEMENT_START);
        start();
        prepared.checkParameters();
        ResultInterface result = prepared.query(maxrows);
        prepared.trace(startTime, result.getRowCount());
        setProgress(DatabaseEventListener.STATE_STATEMENT_END);
        return result;
    }

    @Override
    public boolean isReadOnly() {
        if (!readOnlyKnown) {
            readOnly = prepared.isReadOnly();
            readOnlyKnown = true;
        }
        return readOnly;
    }

    @Override
    public ResultInterface queryMeta() {
        return prepared.queryMeta();
    }

    @Override
    public boolean isCacheable() {
        return prepared.isCacheable();
    }

    @Override
    public int getCommandType() {
        return prepared.getType();
    }

    @Override
    public Prepared getPrepared() {
        return prepared;
    }

}
