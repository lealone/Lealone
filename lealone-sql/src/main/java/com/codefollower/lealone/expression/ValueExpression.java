/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.expression;

import com.codefollower.lealone.dbobject.index.IndexCondition;
import com.codefollower.lealone.dbobject.table.ColumnResolver;
import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueArray;
import com.codefollower.lealone.value.ValueBoolean;
import com.codefollower.lealone.value.ValueNull;

/**
 * An expression representing a constant value.
 */
public class ValueExpression extends Expression {
    /**
     * The expression represents ValueNull.INSTANCE.
     */
    private static final Object NULL = new ValueExpression(ValueNull.INSTANCE);

    /**
     * This special expression represents the default value. It is used for
     * UPDATE statements of the form SET COLUMN = DEFAULT. The value is
     * ValueNull.INSTANCE, but should never be accessed.
     */
    private static final Object DEFAULT = new ValueExpression(ValueNull.INSTANCE);

    private final Value value;

    private ValueExpression(Value value) {
        this.value = value;
    }

    /**
     * Get the NULL expression.
     *
     * @return the NULL expression
     */
    public static ValueExpression getNull() {
        return (ValueExpression) NULL;
    }

    /**
     * Get the DEFAULT expression.
     *
     * @return the DEFAULT expression
     */
    public static ValueExpression getDefault() {
        return (ValueExpression) DEFAULT;
    }

    /**
     * Create a new expression with the given value.
     *
     * @param value the value
     * @return the expression
     */
    public static ValueExpression get(Value value) {
        if (value == ValueNull.INSTANCE) {
            return getNull();
        }
        return new ValueExpression(value);
    }

    public Value getValue(Session session) {
        return value;
    }

    public int getType() {
        return value.getType();
    }

    public void createIndexConditions(Session session, TableFilter filter) {
        if (value.getType() == Value.BOOLEAN) {
            boolean v = ((ValueBoolean) value).getBoolean().booleanValue();
            if (!v) {
                filter.addIndexCondition(IndexCondition.get(Comparison.FALSE, null, this));
            }
        }
    }

    public Expression getNotIfPossible(Session session) {
        return new Comparison(session, Comparison.EQUAL, this, ValueExpression.get(ValueBoolean.get(false)));
    }

    public void mapColumns(ColumnResolver resolver, int level) {
        // nothing to do
    }

    public Expression optimize(Session session) {
        return this;
    }

    public boolean isConstant() {
        return true;
    }

    public boolean isValueSet() {
        return true;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // nothing to do
    }

    public int getScale() {
        return value.getScale();
    }

    public long getPrecision() {
        return value.getPrecision();
    }

    public int getDisplaySize() {
        return value.getDisplaySize();
    }

    public String getSQL(boolean isDistributed) {
        if (this == DEFAULT) {
            return "DEFAULT";
        }
        return value.getSQL();
    }

    public void updateAggregate(Session session) {
        // nothing to do
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.GET_COLUMNS:
            return true;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    public int getCost() {
        return 0;
    }

    public Expression[] getExpressionColumns(Session session) {
        if (getType() == Value.ARRAY) {
            return getExpressionColumns(session, (ValueArray) getValue(session));
        }
        return super.getExpressionColumns(session);
    }
}
