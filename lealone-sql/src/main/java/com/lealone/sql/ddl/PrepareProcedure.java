/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import java.util.ArrayList;

import com.lealone.db.Procedure;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.StatementBase;
import com.lealone.sql.expression.Parameter;

/**
 * This class represents the statement
 * PREPARE
 */
public class PrepareProcedure extends DefinitionStatement {

    private String procedureName;
    private StatementBase prepared;

    public PrepareProcedure(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.PREPARE;
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

    public void setPrepared(StatementBase prep) {
        this.prepared = prep;
    }

    @Override
    public ArrayList<Parameter> getParameters() {
        return new ArrayList<>(0);
    }

    @Override
    public void checkParameters() {
        // no not check parameters
    }

    @Override
    public int update() {
        Procedure proc = new Procedure(procedureName, prepared);
        prepared.setParameterList(parameters);
        prepared.setPrepareAlways(prepareAlways);
        prepared.prepare();
        session.addProcedure(proc);
        return 0;
    }
}
