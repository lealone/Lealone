/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.command.dml;

import java.util.ArrayList;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.dbobject.Procedure;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.expression.Expression;
import com.codefollower.lealone.expression.Parameter;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.util.New;

/**
 * This class represents the statement
 * EXECUTE
 */
public class ExecuteProcedure extends Prepared {

    private final ArrayList<Expression> expressions = New.arrayList();
    private Procedure procedure;

    public ExecuteProcedure(Session session) {
        super(session);
    }

    public void setProcedure(Procedure procedure) {
        this.procedure = procedure;
    }

    /**
     * Set the expression at the given index.
     *
     * @param index the index (0 based)
     * @param expr the expression
     */
    public void setExpression(int index, Expression expr) {
        expressions.add(index, expr);
    }

    private void setParameters() {
        Prepared prepared = procedure.getPrepared();
        ArrayList<Parameter> params = prepared.getParameters();
        for (int i = 0; params != null && i < params.size() && i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            Parameter p = params.get(i);
            p.setValue(expr.getValue(session));
        }
    }

    public boolean isQuery() {
        Prepared prepared = procedure.getPrepared();
        return prepared.isQuery();
    }

    public int update() {
        setParameters();
        Prepared prepared = procedure.getPrepared();
        return prepared.update();
    }

    public ResultInterface query(int limit) {
        setParameters();
        Prepared prepared = procedure.getPrepared();
        return prepared.query(limit);
    }

    public boolean isTransactional() {
        return true;
    }

    public ResultInterface queryMeta() {
        Prepared prepared = procedure.getPrepared();
        return prepared.queryMeta();
    }

    public int getType() {
        return CommandInterface.EXECUTE;
    }

}
