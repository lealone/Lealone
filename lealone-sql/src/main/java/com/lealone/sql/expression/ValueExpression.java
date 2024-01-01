/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression;

import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueBoolean;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.expression.condition.Comparison;
import com.lealone.sql.expression.visitor.ExpressionVisitor;
import com.lealone.sql.optimizer.IndexCondition;
import com.lealone.sql.optimizer.TableFilter;

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

    @Override
    public Value getValue(ServerSession session) {
        return value;
    }

    @Override
    public int getType() {
        return value.getType();
    }

    @Override
    public void createIndexConditions(ServerSession session, TableFilter filter) {
        if (value.getType() == Value.BOOLEAN && !value.getBoolean()) {
            filter.addIndexCondition(IndexCondition.get(Comparison.FALSE, null, this));
        }
    }

    @Override
    public Expression getNotIfPossible(ServerSession session) {
        return new Comparison(session, Comparison.EQUAL, this,
                ValueExpression.get(ValueBoolean.get(false)));
    }

    @Override
    public Expression optimize(ServerSession session) {
        return this;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean isValueSet() {
        return true;
    }

    @Override
    public int getScale() {
        return value.getScale();
    }

    @Override
    public long getPrecision() {
        return value.getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return value.getDisplaySize();
    }

    @Override
    public String getSQL() {
        if (this == DEFAULT) {
            return "DEFAULT";
        }
        return value.getSQL();
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public Expression[] getExpressionColumns(ServerSession session) {
        if (getType() == Value.ARRAY) {
            return getExpressionColumns(session, (ValueArray) getValue(session));
        }
        return super.getExpressionColumns(session);
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitValueExpression(this);
    }
}
