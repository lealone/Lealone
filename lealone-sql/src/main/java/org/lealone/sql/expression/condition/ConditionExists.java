/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.condition;

import org.lealone.common.util.StringUtils;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.visitor.IExpressionVisitor;
import org.lealone.sql.optimizer.ColumnResolver;
import org.lealone.sql.query.Query;

/**
 * An 'exists' condition as in WHERE EXISTS(SELECT ...)
 */
public class ConditionExists extends Condition {

    private final Query query;

    public ConditionExists(Query query) {
        this.query = query;
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public Value getValue(ServerSession session) {
        query.setSession(session);
        Result result = query.query(1);
        session.addTemporaryResult(result);
        boolean r = result.getRowCount() > 0;
        return ValueBoolean.get(r);
    }

    @Override
    public Expression optimize(ServerSession session) {
        query.prepare();
        return this;
    }

    @Override
    public String getSQL(boolean isDistributed) {
        return "EXISTS(\n" + StringUtils.indent(query.getPlanSQL(), 4, false) + ")";
    }

    @Override
    public void updateAggregate(ServerSession session) {
        // TODO exists: is it allowed that the subquery contains aggregates?
        // probably not
        // select id from test group by id having exists (select * from test2
        // where id=count(test.id))
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        query.mapColumns(resolver, level + 1);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return query.getCostAsExpression();
    }

    @Override
    public <R> R accept(IExpressionVisitor<R> visitor) {
        return visitor.visitConditionExists(this);
    }
}
