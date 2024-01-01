/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

public abstract class ExpressionVisitorBase<R> implements ExpressionVisitor<R> {

    private int queryLevel;

    @Override
    public ExpressionVisitorBase<R> incrementQueryLevel(int offset) {
        ExpressionVisitorBase<R> c = copy();
        c.setQueryLevel(getQueryLevel() + offset);
        return c;
    }

    public void setQueryLevel(int queryLevel) {
        this.queryLevel = queryLevel;
    }

    @Override
    public int getQueryLevel() {
        return queryLevel;
    }

    protected ExpressionVisitorBase<R> copy() {
        return this;
    }
}
