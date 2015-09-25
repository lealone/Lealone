/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import org.lealone.common.value.Value;
import org.lealone.common.value.ValueNull;
import org.lealone.db.Session;
import org.lealone.db.expression.ExpressionVisitor;
import org.lealone.db.table.ColumnResolver;
import org.lealone.db.table.TableFilter;

/**
 * A NOT condition.
 */
public class ConditionNot extends Condition {

    private Expression condition;

    public ConditionNot(Expression condition) {
        this.condition = condition;
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        return condition;
    }

    @Override
    public Value getValue(Session session) {
        Value v = condition.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return v;
        }
        return v.convertTo(Value.BOOLEAN).negate();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        condition.mapColumns(resolver, level);
    }

    @Override
    public Expression optimize(Session session) {
        Expression e2 = condition.getNotIfPossible(session);
        if (e2 != null) {
            return e2.optimize(session);
        }
        Expression expr = condition.optimize(session);
        if (expr.isConstant()) {
            Value v = expr.getValue(session);
            if (v == ValueNull.INSTANCE) {
                return ValueExpression.getNull();
            }
            return ValueExpression.get(v.convertTo(Value.BOOLEAN).negate());
        }
        condition = expr;
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        condition.setEvaluatable(tableFilter, b);
    }

    @Override
    public String getSQL(boolean isDistributed) {
        return "(NOT " + condition.getSQL(isDistributed) + ")";
    }

    @Override
    public void updateAggregate(Session session) {
        condition.updateAggregate(session);
    }

    @Override
    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        if (outerJoin) {
            // can not optimize:
            // select * from test t1 left join test t2 on t1.id = t2.id where
            // not t2.id is not null
            // to
            // select * from test t1 left join test t2 on t1.id = t2.id and
            // t2.id is not null
            return;
        }
        super.addFilterConditions(filter, outerJoin);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return condition.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return condition.getCost();
    }

}
