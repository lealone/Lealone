/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.expression;

import org.lealone.common.message.DbException;
import org.lealone.common.value.Value;
import org.lealone.common.value.ValueInt;
import org.lealone.common.value.ValueLong;
import org.lealone.db.Session;
import org.lealone.db.schema.Sequence;
import org.lealone.db.table.ColumnResolver;
import org.lealone.db.table.TableFilter;

/**
 * Wraps a sequence when used in a statement.
 */
public class SequenceValue implements Expression {

    private final Sequence sequence;

    public SequenceValue(Sequence sequence) {
        this.sequence = sequence;
    }

    @Override
    public Value getValue(Session session) {
        long value = sequence.getNext(session);
        session.setLastIdentity(ValueLong.get(value));
        return ValueLong.get(value);
    }

    @Override
    public int getType() {
        return Value.LONG;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        // nothing to do
    }

    @Override
    public Expression optimize(Session session) {
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // nothing to do
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public long getPrecision() {
        return ValueInt.PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return ValueInt.DISPLAY_SIZE;
    }

    public String getSQL(boolean isDistributed) {
        return "(NEXT VALUE FOR " + sequence.getSQL() + ")";
    }

    public void updateAggregate(Session session) {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.GET_COLUMNS:
            return true;
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return false;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            visitor.addDataModificationId(sequence.getModificationId());
            return true;
        case ExpressionVisitor.GET_DEPENDENCIES:
            visitor.addDependency(sequence);
            return true;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    public int getCost() {
        return 1;
    }

    @Override
    public String getAlias() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTableName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSchemaName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getColumnName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getNullable() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isAutoIncrement() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getSQL() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Expression getNonAliasExpression() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isConstant() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void addFilterConditions(TableFilter filter, boolean outerJoin) {
        // TODO Auto-generated method stub

    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        // TODO Auto-generated method stub

    }

    @Override
    public Boolean getBooleanValue(Session session) {
        // TODO Auto-generated method stub
        return null;
    }

}
