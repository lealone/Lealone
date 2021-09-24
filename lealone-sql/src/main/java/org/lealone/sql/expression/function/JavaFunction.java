/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.function;

import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Constants;
import org.lealone.db.schema.FunctionAlias;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueResultSet;
import org.lealone.sql.Parser;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.visitor.ExpressionVisitor;

/**
 * This class wraps a user-defined function.
 * 
 * @author H2 Group
 * @author zhh
 */
public class JavaFunction extends Function {

    private final FunctionAlias functionAlias;
    private final FunctionAlias.JavaMethod javaMethod;

    public JavaFunction(FunctionAlias functionAlias, Expression[] args) {
        this.functionAlias = functionAlias;
        this.javaMethod = functionAlias.findJavaMethod(args);
        this.args = args;
    }

    public FunctionAlias getFunctionAlias() {
        return functionAlias;
    }

    @Override
    public Value getValue(ServerSession session) {
        return javaMethod.getValue(session, args, false);
    }

    @Override
    public int getType() {
        return javaMethod.getDataType();
    }

    @Override
    public Expression optimize(ServerSession session) {
        boolean allConst = optimizeArgs(session);
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public int getScale() {
        return DataType.getDataType(getType()).defaultScale;
    }

    @Override
    public long getPrecision() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getDisplaySize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getSQL(boolean isDistributed) {
        StatementBuilder buff = new StatementBuilder();
        if (!functionAlias.getSchema().getName().equals(Constants.SCHEMA_MAIN)) {
            buff.append(Parser.quoteIdentifier(functionAlias.getSchema().getName())).append('.');
        }
        buff.append(Parser.quoteIdentifier(functionAlias.getName())).append('(');
        appendArgs(buff, isDistributed);
        return buff.append(')').toString();
    }

    @Override
    public String getName() {
        return functionAlias.getName();
    }

    @Override
    public ValueResultSet getValueForColumnList(ServerSession session, Expression[] args) {
        Value v = javaMethod.getValue(session, args, true);
        return v == ValueNull.INSTANCE ? null : (ValueResultSet) v;
    }

    @Override
    public int getCost() {
        int cost = javaMethod.hasConnectionParam() ? 25 : 5;
        for (Expression e : args) {
            cost += e.getCost();
        }
        return cost;
    }

    @Override
    public Expression[] getExpressionColumns(ServerSession session) {
        switch (getType()) {
        case Value.RESULT_SET:
            ValueResultSet rs = getValueForColumnList(session, getArgs());
            return getExpressionColumns(session, rs.getResultSet());
        case Value.ARRAY:
            return getExpressionColumns(session, (ValueArray) getValue(session));
        }
        return super.getExpressionColumns(session);
    }

    @Override
    public int getFunctionType() {
        return -1;
    }

    @Override
    public boolean isDeterministic() {
        return functionAlias.isDeterministic();
    }

    @Override
    boolean isBufferResultSetToLocalTemp() {
        return functionAlias.isBufferResultSetToLocalTemp();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitJavaFunction(this);
    }
}
