/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.aggregate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.api.Aggregate;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.UserAggregate;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.Parser;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.visitor.IExpressionVisitor;
import org.lealone.sql.query.Select;

/**
 * This class wraps a user-defined aggregate.
 * 
 * @author H2 Group
 * @author zhh
 */
public class JavaAggregate extends org.lealone.sql.expression.aggregate.Aggregate {

    private final UserAggregate userAggregate;
    private final Expression[] args;
    private int[] argTypes;
    private Connection userConnection;

    private Aggregate aggregate;

    public JavaAggregate(UserAggregate userAggregate, Expression[] args, Select select) {
        super(select);
        this.userAggregate = userAggregate;
        this.args = args;
    }

    public UserAggregate getUserAggregate() {
        return userAggregate;
    }

    public Expression[] getArgs() {
        return args;
    }

    @Override
    public int getScale() {
        return DataType.getDataType(dataType).defaultScale;
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
    public int getCost() {
        int cost = 5;
        for (Expression e : args) {
            cost += e.getCost();
        }
        return cost;
    }

    @Override
    public Expression optimize(ServerSession session) {
        userConnection = session.createConnection(false);
        int len = args.length;
        argTypes = new int[len];
        for (int i = 0; i < len; i++) {
            Expression expr = args[i];
            args[i] = expr.optimize(session);
            int type = expr.getType();
            argTypes[i] = type;
        }
        try {
            Aggregate aggregate = getInstance();
            dataType = aggregate.getInternalType(argTypes);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        return this;
    }

    @Override
    public String getSQL(boolean isDistributed) {
        StatementBuilder buff = new StatementBuilder();
        buff.append(Parser.quoteIdentifier(userAggregate.getName())).append('(');
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL(isDistributed));
        }
        return buff.append(')').toString();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            // TODO optimization: some functions are deterministic, but we don't
            // know (no setting for that)
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            // user defined aggregate functions can not be optimized
            return false;
        case ExpressionVisitor.GET_DEPENDENCIES:
            visitor.addDependency(userAggregate);
            break;
        default:
        }
        for (Expression e : args) {
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    private Aggregate getInstance() throws SQLException {
        if (aggregate == null) {
            aggregate = userAggregate.getInstance();
            aggregate.init(userConnection);
        }
        return aggregate;
    }

    @Override
    public Value getValue(ServerSession session) {
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        try {
            Aggregate agg = (Aggregate) group.get(this);
            if (agg == null) {
                agg = getInstance();
            }
            Object obj = agg.getResult();
            if (obj == null) {
                return ValueNull.INSTANCE;
            }
            return DataType.convertToValue(session, obj, dataType);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void updateAggregate(ServerSession session) {
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = select.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        Aggregate agg = (Aggregate) group.get(this);
        try {
            if (agg == null) {
                agg = getInstance();
                group.put(this, agg);
            }
            Object[] argValues = new Object[args.length];
            Object arg = null;
            for (int i = 0, len = args.length; i < len; i++) {
                Value v = args[i].getValue(session);
                v = v.convertTo(argTypes[i]);
                arg = v.getObject();
                argValues[i] = arg;
            }
            if (args.length == 1) {
                agg.add(arg);
            } else {
                agg.add(argValues);
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public <R> R accept(IExpressionVisitor<R> visitor) {
        return visitor.visitJavaAggregate(this);
    }
}
