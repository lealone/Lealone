/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.sql.LealoneSQLParser;
import org.lealone.sql.expression.visitor.ExpressionVisitor;

/**
 * A column alias as in SELECT 'Hello' AS NAME ...
 */
public class Alias extends Expression {

    private final String alias;
    private Expression expr;
    private final boolean aliasColumnName;

    public Alias(Expression expression, String alias, boolean aliasColumnName) {
        this.expr = expression;
        this.alias = alias;
        this.aliasColumnName = aliasColumnName;
    }

    @Override
    public Expression getNonAliasExpression() {
        return expr;
    }

    @Override
    public Value getValue(ServerSession session) {
        return expr.getValue(session);
    }

    @Override
    public int getType() {
        return expr.getType();
    }

    @Override
    public Expression optimize(ServerSession session) {
        expr = expr.optimize(session);
        return this;
    }

    @Override
    public int getScale() {
        return expr.getScale();
    }

    @Override
    public long getPrecision() {
        return expr.getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return expr.getDisplaySize();
    }

    @Override
    public boolean isAutoIncrement() {
        return expr.isAutoIncrement();
    }

    @Override
    public String getSQL() {
        return expr.getSQL() + " AS " + LealoneSQLParser.quoteIdentifier(alias);
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public int getNullable() {
        return expr.getNullable();
    }

    @Override
    public int getCost() {
        return expr.getCost();
    }

    @Override
    public String getTableName() {
        if (aliasColumnName) {
            return super.getTableName();
        }
        return expr.getTableName();
    }

    @Override
    public String getColumnName() {
        if (!(expr instanceof ExpressionColumn) || aliasColumnName) {
            return super.getColumnName();
        }
        return expr.getColumnName();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitAlias(this);
    }
}
