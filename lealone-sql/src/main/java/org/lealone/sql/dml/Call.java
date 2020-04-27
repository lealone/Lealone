/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import java.sql.ResultSet;

import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.expression.Expression;

/**
 * This class represents the statement
 * CALL.
 */
public class Call extends ManipulationStatement {

    private boolean isResultSet;
    private Expression expression;
    private Expression[] expressions;

    public Call(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.CALL;
    }

    @Override
    public boolean isCacheable() {
        return !isResultSet;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public Result getMetaData() {
        LocalResult result;
        if (isResultSet) {
            Expression[] expr = expression.getExpressionColumns(session);
            result = new LocalResult(session, expr, expr.length);
        } else {
            result = new LocalResult(session, expressions, 1);
        }
        result.done();
        return result;
    }

    @Override
    public PreparedSQLStatement prepare() {
        expression = expression.optimize(session);
        expressions = new Expression[] { expression };
        isResultSet = expression.getType() == Value.RESULT_SET;
        if (isResultSet) {
            prepareAlways = true;
        }
        return this;
    }

    @Override
    public int update() {
        Value v = expression.getValue(session);
        int type = v.getType();
        switch (type) {
        case Value.RESULT_SET:
            // this will throw an exception
            // methods returning a result set may not be called like this.
            return super.update();
        case Value.UNKNOWN:
        case Value.NULL:
            return 0;
        default:
            return v.getInt();
        }
    }

    @Override
    public Result query(int maxRows) {
        setCurrentRowNumber(1);
        Value v = expression.getValue(session);
        if (isResultSet) {
            v = v.convertTo(Value.RESULT_SET);
            ResultSet rs = v.getResultSet();
            return LocalResult.read(session, Expression.getExpressionColumns(session, rs), rs, maxRows);
        }
        LocalResult result = new LocalResult(session, expressions, 1);
        Value[] row = { v };
        result.addRow(row);
        result.done();
        return result;
    }

}
