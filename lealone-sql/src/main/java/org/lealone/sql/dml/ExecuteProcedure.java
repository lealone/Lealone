/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import java.util.ArrayList;

import org.lealone.db.Procedure;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Parameter;

/**
 * This class represents the statement
 * EXECUTE
 */
public class ExecuteProcedure extends ManipulationStatement {

    private final ArrayList<Expression> expressions = new ArrayList<>();
    private Procedure procedure;

    public ExecuteProcedure(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.EXECUTE;
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

    @Override
    public boolean isQuery() {
        return stmt().isQuery();
    }

    @Override
    public Result getMetaData() {
        return stmt().getMetaData();
    }

    private StatementBase stmt() {
        return (StatementBase) procedure.getPrepared();
    }

    private void setParameters() {
        ArrayList<Parameter> params = stmt().getParameters();
        if (params == null)
            return;
        int size = Math.min(params.size(), expressions.size());
        for (int i = 0; i < size; i++) {
            Expression expr = expressions.get(i);
            Parameter p = params.get(i);
            p.setValue(expr.getValue(session));
        }
    }

    @Override
    public int update() {
        setParameters();
        return stmt().update();
    }

    @Override
    public Result query(int limit) {
        setParameters();
        return stmt().query(limit);
    }
}
