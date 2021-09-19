/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.function;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Database;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueResultSet;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.visitor.IExpressionVisitor;
import org.lealone.sql.optimizer.ColumnResolver;

/**
 * This class implements most built-in functions of this database.
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class BuiltInFunction extends Function {

    protected final Database database;
    protected final FunctionInfo info;
    protected Expression[] args;
    private ArrayList<Expression> varArgs;
    protected int dataType, scale;
    protected long precision = PRECISION_UNKNOWN;
    protected int displaySize;

    protected BuiltInFunction(Database database, FunctionInfo info) {
        this.database = database;
        this.info = info;
        if (info.parameterCount == VAR_ARGS) {
            varArgs = new ArrayList<>(4);
        } else {
            args = new Expression[info.parameterCount];
        }
    }

    /**
     * Set the parameter expression at the given index.
     *
     * @param index the index (0, 1,...)
     * @param param the expression
     */
    @Override
    public void setParameter(int index, Expression param) {
        if (varArgs != null) {
            varArgs.add(param);
        } else {
            if (index >= args.length) {
                throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, "" + args.length);
            }
            args[index] = param;
        }
    }

    @Override
    public Value getValue(ServerSession session) {
        return getValueWithArgs(session, args);
    }

    private Value getValueWithArgs(ServerSession session, Expression[] args) {
        Value[] values = new Value[args.length];
        if (info.nullIfParameterIsNull) { // 如果函数要求所有的参数非null，那么只要有一个参数是null，函数就直接返回null
            for (int i = 0; i < args.length; i++) {
                Expression e = args[i];
                Value v = e.getValue(session);
                if (v == ValueNull.INSTANCE) {
                    return ValueNull.INSTANCE;
                }
                values[i] = v;
            }
        }
        Value v0 = getNullOrValue(session, args, values, 0);
        Value resultSimple = getSimpleValue(session, v0, args, values);
        if (resultSimple != null) {
            return resultSimple;
        }
        return getValue(session, args, values);
    }

    protected Value getSimpleValue(ServerSession session, Value v0, Expression[] args, Value[] values) {
        return null;
    }

    protected Value getValue(ServerSession session, Expression[] args, Value[] values) {
        return null;
    }

    protected static Value getNullOrValue(ServerSession session, Expression[] args, Value[] values, int i) {
        if (i >= args.length) {
            return null;
        }
        Value v = values[i];
        if (v == null) {
            v = values[i] = args[i].getValue(session);
        }
        return v;
    }

    @Override
    public int getType() {
        return dataType;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression e : args) {
            if (e != null) {
                e.mapColumns(resolver, level);
            }
        }
    }

    /**
     * Check if the parameter count is correct.
     *
     * @param len the number of parameters set
     * @throws DbException if the parameter count is incorrect
     */
    protected abstract void checkParameterCount(int len);

    protected void checkParameterCount(int len, int min, int max) {
        boolean ok = (len >= min) && (len <= max);
        if (!ok) {
            throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, min + ".." + max);
        }
    }

    /**
     * This method is called after all the parameters have been set.
     * It checks if the parameter count is correct.
     *
     * @throws DbException if the parameter count is incorrect.
     */
    @Override
    public void doneWithParameters() {
        if (info.parameterCount == VAR_ARGS) {
            int len = varArgs.size();
            checkParameterCount(len);
            args = new Expression[len];
            varArgs.toArray(args);
            varArgs = null;
        } else {
            int len = args.length;
            if (len > 0 && args[len - 1] == null) {
                throw DbException.get(ErrorCode.INVALID_PARAMETER_COUNT_2, info.name, "" + len);
            }
        }
    }

    @Override
    public void setDataType(Column col) {
        dataType = col.getType();
        precision = col.getPrecision();
        displaySize = col.getDisplaySize();
        scale = col.getScale();
    }

    protected boolean optimizeArgs(ServerSession session) {
        boolean allConst = info.deterministic;
        for (int i = 0; i < args.length; i++) {
            Expression e = args[i];
            if (e == null) {
                continue;
            }
            e = e.optimize(session);
            args[i] = e;
            if (!e.isConstant()) {
                allConst = false;
            }
        }
        return allConst;
    }

    @Override
    public Expression optimize(ServerSession session) {
        boolean allConst = optimizeArgs(session);
        dataType = info.dataType;
        DataType type = DataType.getDataType(dataType);
        precision = PRECISION_UNKNOWN;
        scale = 0;
        displaySize = type.defaultScale;
        if (allConst) {
            Value v = getValue(session);
            return ValueExpression.get(v);
        }
        return this;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public long getPrecision() {
        if (precision == PRECISION_UNKNOWN) {
            calculatePrecisionAndDisplaySize();
        }
        return precision;
    }

    @Override
    public int getDisplaySize() {
        if (precision == PRECISION_UNKNOWN) {
            calculatePrecisionAndDisplaySize();
        }
        return displaySize;
    }

    protected void calculatePrecisionAndDisplaySize() {
        DataType type = DataType.getDataType(dataType);
        precision = type.defaultPrecision;
        displaySize = type.defaultDisplaySize;
    }

    @Override
    public String getSQL(boolean isDistributed) {
        StatementBuilder buff = new StatementBuilder(info.name);
        buff.append('(');
        appendArgs(buff, isDistributed);
        return buff.append(')').toString();
    }

    protected void appendArgs(StatementBuilder buff, boolean isDistributed) {
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL(isDistributed));
        }
    }

    @Override
    public void updateAggregate(ServerSession session) {
        for (Expression e : args) {
            if (e != null) {
                e.updateAggregate(session);
            }
        }
    }

    @Override
    public int getFunctionType() {
        return info.type;
    }

    @Override
    public String getName() {
        return info.name;
    }

    @Override
    public ValueResultSet getValueForColumnList(ServerSession session, Expression[] argList) {
        return (ValueResultSet) getValueWithArgs(session, argList);
    }

    @Override
    public Expression[] getArgs() {
        return args;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        for (Expression e : args) {
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return info.deterministic;
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
        case ExpressionVisitor.GET_COLUMNS:
            return true;
        default:
            throw DbException.getInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {
        int cost = 3;
        for (Expression e : args) {
            cost += e.getCost();
        }
        return cost;
    }

    @Override
    public boolean isDeterministic() {
        return info.deterministic;
    }

    @Override
    public boolean isBufferResultSetToLocalTemp() {
        return info.bufferResultSetToLocalTemp;
    }

    @Override
    public <R> R accept(IExpressionVisitor<R> visitor) {
        return visitor.visitFunction(this);
    }
}