/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression.condition;

import java.util.ArrayList;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.Database;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueBoolean;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.ValueExpression;
import com.lealone.sql.expression.visitor.ExpressionVisitor;
import com.lealone.sql.expression.visitor.ExpressionVisitorFactory;
import com.lealone.sql.expression.visitor.NotFromResolverVisitor;
import com.lealone.sql.optimizer.IndexCondition;
import com.lealone.sql.optimizer.TableFilter;

/**
 * An 'in' condition with a list of values, as in WHERE NAME IN(...)
 */
public class ConditionIn extends Condition {

    private final Database database;
    private Expression left;
    private final ArrayList<Expression> valueList;
    private int queryLevel;

    /**
     * Create a new IN(..) condition.
     *
     * @param database the database
     * @param left the expression before IN
     * @param values the value list (at least one element)
     */
    public ConditionIn(Database database, Expression left, ArrayList<Expression> values) {
        this.database = database;
        this.left = left;
        this.valueList = values;
    }

    public Expression getLeft() {
        return left;
    }

    public ArrayList<Expression> getValueList() {
        return valueList;
    }

    public void setQueryLevel(int level) {
        this.queryLevel = Math.max(level, this.queryLevel);
    }

    @Override
    public Value getValue(ServerSession session) {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return l;
        }
        boolean result = false;
        boolean hasNull = false;
        for (Expression e : valueList) {
            Value r = e.getValue(session);
            if (r == ValueNull.INSTANCE) {
                hasNull = true;
            } else {
                r = r.convertTo(l.getType());
                result = Comparison.compareNotNull(database, l, r, Comparison.EQUAL);
                if (result) {
                    break;
                }
            }
        }
        if (!result && hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(result);
    }

    @Override
    public Expression optimize(ServerSession session) {
        left = left.optimize(session);
        boolean constant = left.isConstant();
        if (constant && left == ValueExpression.getNull()) {
            return left;
        }
        boolean allValuesConstant = true;
        boolean allValuesNull = true;
        int size = valueList.size();
        for (int i = 0; i < size; i++) {
            Expression e = valueList.get(i);
            e = e.optimize(session);
            if (e.isConstant() && e.getValue(session) != ValueNull.INSTANCE) {
                allValuesNull = false;
            }
            if (allValuesConstant && !e.isConstant()) {
                allValuesConstant = false;
            }
            valueList.set(i, e);
        }
        if (constant && allValuesConstant) {
            return ValueExpression.get(getValue(session));
        }
        if (size == 1) {
            Expression right = valueList.get(0);
            Expression expr = new Comparison(session, Comparison.EQUAL, left, right);
            expr = expr.optimize(session);
            return expr;
        }
        if (allValuesConstant && !allValuesNull) {
            int leftType = left.getType();
            if (leftType == Value.UNKNOWN) {
                return this;
            }
            Expression expr = new ConditionInConstantSet(session, left, valueList);
            expr = expr.optimize(session);
            return expr;
        }
        return this;
    }

    @Override
    public void createIndexConditions(ServerSession session, TableFilter filter) {
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        if (session.getDatabase().getSettings().optimizeInList) {
            NotFromResolverVisitor visitor = ExpressionVisitorFactory.getNotFromResolverVisitor(filter);
            for (Expression e : valueList) {
                if (!e.accept(visitor)) {
                    return;
                }
            }
            filter.addIndexCondition(IndexCondition.getInList(l, valueList));
            return;
        }
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        buff.append(left.getSQL()).append(" IN(");
        for (Expression e : valueList) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL());
        }
        return buff.append("))").toString();
    }

    @Override
    public int getCost() {
        int cost = left.getCost();
        for (Expression e : valueList) {
            cost += e.getCost();
        }
        return cost;
    }

    /**
     * Add an additional element if possible. Example: given two conditions
     * A IN(1, 2) OR A=3, the constant 3 is added: A IN(1, 2, 3).
     *
     * @param other the second condition
     * @return null if the condition was not added, or the new condition
     */
    Expression getAdditional(Comparison other) {
        Expression add = other.getIfEquals(left);
        if (add != null) {
            valueList.add(add);
            return this;
        }
        return null;
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitConditionIn(this);
    }
}
