/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

public abstract class ExpressionVisitorBase<R> implements ExpressionVisitor<R> {

    private int queryLevel;

    @Override
    public ExpressionVisitor<R> incrementQueryLevel(int offset) {
        setQueryLevel(queryLevel + offset);
        return copy(queryLevel);
    }

    public void setQueryLevel(int queryLevel) {
        this.queryLevel = queryLevel;
    }

    @Override
    public int getQueryLevel() {
        return queryLevel;
    }

    protected ExpressionVisitorBase<R> copy(int newQueryLevel) {
        return this;
    }
}
