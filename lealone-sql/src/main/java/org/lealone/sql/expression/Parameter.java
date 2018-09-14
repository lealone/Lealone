/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.CommandParameter;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.expression.ExpressionVisitor;
import org.lealone.db.table.Column;
import org.lealone.db.table.ColumnResolver;
import org.lealone.db.table.TableFilter;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueNull;

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
    public String getSQL(boolean isDistributed) {
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
    public void mapColumns(ColumnResolver resolver, int level) {
        // can't map
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
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // not bound
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
    public void updateAggregate(ServerSession session) {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.EVALUATABLE:
            // the parameter _will_be_ evaluatable at execute time
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            // it is checked independently if the value is the same as the last time
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.GET_COLUMNS:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return value != null;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public Expression getNotIfPossible(ServerSession session) {
        return new Comparison(session, Comparison.EQUAL, this, ValueExpression.get(ValueBoolean.get(false)));
    }

    public void setColumn(Column column) {
        this.column = column;
    }

}
