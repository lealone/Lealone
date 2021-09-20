/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import java.util.TreeSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.sql.Parser;
import org.lealone.sql.expression.evaluator.HotSpotEvaluator;
import org.lealone.sql.expression.visitor.IExpressionVisitor;

/**
 * A user-defined variable, for example: @ID.
 */
public class Variable extends Expression {

    private final String name;
    private Value lastValue;

    public Variable(ServerSession session, String name) {
        this.name = name;
        lastValue = session.getVariable(name);
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public int getDisplaySize() {
        return lastValue.getDisplaySize();
    }

    @Override
    public long getPrecision() {
        return lastValue.getPrecision();
    }

    @Override
    public String getSQL(boolean isDistributed) {
        return "@" + Parser.quoteIdentifier(name);
    }

    @Override
    public int getScale() {
        return lastValue.getScale();
    }

    @Override
    public int getType() {
        return lastValue.getType();
    }

    @Override
    public Value getValue(ServerSession session) {
        lastValue = session.getVariable(name);
        return lastValue;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.EVALUATABLE:
            // the value will be evaluated at execute time
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            // it is checked independently if the value is the same as the last time
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.GET_COLUMNS:
            return true;
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        default:
            throw DbException.getInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public Expression optimize(ServerSession session) {
        return this;
    }

    public String getName() {
        return name;
    }

    @Override
    public void genCode(HotSpotEvaluator evaluator, StringBuilder buff, TreeSet<String> importSet, int level,
            String retVar) {
        StringBuilder indent = indent((level + 1) * 4);
        buff.append(indent).append(retVar).append(" = session.getVariable(\"").append(name).append("\");\r\n");
    }

    @Override
    public <R> R accept(IExpressionVisitor<R> visitor) {
        return visitor.visitVariable(this);
    }
}
