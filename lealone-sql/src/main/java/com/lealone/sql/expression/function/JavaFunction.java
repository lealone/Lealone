/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression.function;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.Constants;
import com.lealone.db.schema.FunctionAlias;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueResultSet;
import com.lealone.sql.LealoneSQLParser;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ValueExpression;
import com.lealone.sql.expression.visitor.ExpressionVisitor;

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
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder();
        if (!functionAlias.getSchema().getName().equals(Constants.SCHEMA_MAIN)) {
            buff.append(LealoneSQLParser.quoteIdentifier(functionAlias.getSchema().getName()))
                    .append('.');
        }
        buff.append(LealoneSQLParser.quoteIdentifier(functionAlias.getName())).append('(');
        appendArgs(buff);
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
