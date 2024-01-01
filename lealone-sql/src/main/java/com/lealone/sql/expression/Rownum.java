/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression;

import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueInt;
import com.lealone.sql.StatementBase;
import com.lealone.sql.expression.visitor.ExpressionVisitor;

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
    public Expression optimize(ServerSession session) {
        return this;
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
    public String getSQL() {
        return "ROWNUM()";
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitRownum(this);
    }
}
