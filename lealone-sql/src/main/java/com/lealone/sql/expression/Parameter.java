/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.CommandParameter;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueBoolean;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.expression.condition.Comparison;
import com.lealone.sql.expression.visitor.ExpressionVisitor;

/**
 * A parameter of a prepared statement.
 */
public class Parameter extends Expression implements CommandParameter {

    private Value value;
    private Column column;
    private final int index;

    public Parameter(int index) {
        this.index = index;
    }

    @Override
    public String getSQL() {
        return "?" + (index + 1);
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void setValue(Value v, boolean closeOld) {
        // don't need to close the old value as temporary files are anyway removed
        this.value = v;
    }

    @Override
    public void setValue(Value v) {
        this.value = v;
    }

    @Override
    public Value getValue() {
        if (value == null) {
            // to allow parameters in function tables
            return ValueNull.INSTANCE;
        }
        return value;
    }

    @Override
    public Value getValue(ServerSession session) {
        return getValue();
    }

    @Override
    public int getType() {
        if (value != null) {
            return value.getType();
        }
        if (column != null) {
            return column.getType();
        }
        return Value.UNKNOWN;
    }

    @Override
    public void checkSet() {
        if (value == null) {
            throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
        }
    }

    @Override
    public boolean isValueSet() {
        return value != null;
    }

    @Override
    public Expression optimize(ServerSession session) {
        return this;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public int getScale() {
        if (value != null) {
            return value.getScale();
        }
        if (column != null) {
            return column.getScale();
        }
        return 0;
    }

    @Override
    public long getPrecision() {
        if (value != null) {
            return value.getPrecision();
        }
        if (column != null) {
            return column.getPrecision();
        }
        return 0;
    }

    @Override
    public int getDisplaySize() {
        if (value != null) {
            return value.getDisplaySize();
        }
        if (column != null) {
            return column.getDisplaySize();
        }
        return 0;
    }

    @Override
    public int getNullable() {
        if (column != null) {
            return column.isNullable() ? Column.NULLABLE : Column.NOT_NULLABLE;
        }
        return super.getNullable();
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public Expression getNotIfPossible(ServerSession session) {
        return new Comparison(session, Comparison.EQUAL, this,
                ValueExpression.get(ValueBoolean.get(false)));
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitParameter(this);
    }
}
