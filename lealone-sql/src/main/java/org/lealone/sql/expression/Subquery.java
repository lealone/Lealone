/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.dml.Query;
import org.lealone.sql.optimizer.ColumnResolver;
import org.lealone.sql.optimizer.TableFilter;

/**
 * A query returning a single value.
 * Subqueries are used inside other statements.
 */
public class Subquery extends Expression {

    private final Query query;
    private Expression expression;

    public Subquery(Query query) {
        this.query = query;
    }

    @Override
    public Value getValue(ServerSession session) {
        query.setSession(session);
        Result result = query.query(2);// session.createSubqueryResult(query, 2); // query.query(2);
        try {
            int rowcount = result.getRowCount();
            if (rowcount > 1) {
                throw DbException.get(ErrorCode.SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW);
            }
            Value v;
            if (rowcount <= 0) {
                v = ValueNull.INSTANCE;
            } else {
                result.next();
                Value[] values = result.currentRow();
                if (result.getVisibleColumnCount() == 1) {
                    v = values[0];
                } else {
                    v = ValueArray.get(values);
                }
            }
            return v;
        } finally {
            result.close();
        }
    }

    @Override
    public int getType() {
        return getExpression().getType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        query.mapColumns(resolver, level + 1);
    }

    @Override
    public Expression optimize(ServerSession session) {
        query.prepare();
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        query.setEvaluatable(tableFilter, b);
    }

    @Override
    public int getScale() {
        return getExpression().getScale();
    }

    @Override
    public long getPrecision() {
        return getExpression().getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return getExpression().getDisplaySize();
    }

    @Override
    public String getSQL(boolean isDistributed) {
        return "(" + query.getPlanSQL() + ")";
    }

    @Override
    public void updateAggregate(ServerSession session) {
        query.updateAggregate(session);
    }

    private Expression getExpression() {
        if (expression == null) {
            ArrayList<Expression> expressions = query.getExpressions();
            int columnCount = query.getColumnCount();
            if (columnCount == 1) {
                expression = expressions.get(0);
            } else {
                Expression[] list = new Expression[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    list[i] = expressions.get(i);
                }
                expression = new ExpressionList(list);
            }
        }
        return expression;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public int getCost() {
        return query.getCostAsExpression();
    }

    @Override
    public Expression[] getExpressionColumns(ServerSession session) {
        return getExpression().getExpressionColumns(session);
    }
}
