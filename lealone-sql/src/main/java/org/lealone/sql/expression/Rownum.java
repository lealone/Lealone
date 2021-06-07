/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.sql.StatementBase;
import org.lealone.sql.optimizer.ColumnResolver;
import org.lealone.sql.optimizer.TableFilter;

/**
 * Represents the ROWNUM function.
 */
public class Rownum extends Expression {

    private final StatementBase prepared;

    public Rownum(StatementBase prepared) {
        this.prepared = prepared;
    }

    @Override
    public Value getValue(ServerSession session) {
        return ValueInt.get(prepared.getCurrentRowNumber());
    }

    @Override
    public int getType() {
        return Value.INT;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        // nothing to do
    }

    @Override
    public Expression optimize(ServerSession session) {
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

    @Override
    public String getSQL(boolean isDistributed) {
        return "ROWNUM()";
    }

    @Override
    public void updateAggregate(ServerSession session) {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.INDEPENDENT:
            return false;
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
        case ExpressionVisitor.GET_COLUMNS:
            // if everything else is the same, the rownum is the same
            return true;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 0;
    }

}
