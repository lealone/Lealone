/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.condition;

import java.util.TreeSet;

import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.evaluator.HotSpotEvaluator;
import org.lealone.sql.expression.visitor.IExpressionVisitor;
import org.lealone.sql.optimizer.TableFilter;

/**
 * A NOT condition.
 */
public class ConditionNot extends Condition {

    private Expression condition;

    public ConditionNot(Expression condition) {
        this.condition = condition;
    }

    public Expression getCondition() {
        return condition;
    }

    @Override
    public Expression getNotIfPossible(ServerSession session) {
        return condition;
    }

    @Override
    public Value getValue(ServerSession session) {
        Value v = condition.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return v;
        }
        return v.convertTo(Value.BOOLEAN).negate();
    }

    @Override
    public Expression optimize(ServerSession session) {
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
    public String getSQL(boolean isDistributed) {
        return "(NOT " + condition.getSQL(isDistributed) + ")";
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

    @Override
    public void genCode(HotSpotEvaluator evaluator, StringBuilder buff, TreeSet<String> importSet, int level,
            String retVar) {
        importSet.add(ValueNull.class.getName());
        importSet.add(Value.class.getName());

        StringBuilder indent = indent((level + 1) * 4);

        buff.append(indent).append("{\r\n");
        String retVarLeft = "lret" + (level + 1);
        buff.append(indent).append("    Value ").append(retVarLeft).append(";\r\n");
        condition.genCode(evaluator, buff, importSet, level + 1, retVarLeft);

        buff.append("    ").append(indent).append(retVar).append(" = ").append(retVarLeft)
                .append(" == ValueNull.INSTANCE ? ").append(retVarLeft).append(" : ").append(retVarLeft)
                .append(".convertTo(Value.BOOLEAN).negate();\r\n");
        buff.append(indent).append("}").append("\r\n");
    }

    @Override
    public <R> R accept(IExpressionVisitor<R> visitor) {
        return visitor.visitConditionNot(this);
    }
}
