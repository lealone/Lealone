/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.dml;

import org.lealone.db.auth.Right;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.service.Service;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ValueExpression;

/**
 * This class represents the statement
 * EXECUTE SERVICE.
 */
public class ExecuteService extends ExecuteStatement {

    private final String serviceName;
    private final String methodName;
    private final Expression[] resultExpressions;

    public ExecuteService(ServerSession session, String serviceName, String methodName) {
        super(session);
        this.serviceName = serviceName;
        this.methodName = methodName;
        ValueExpression e = ValueExpression.get(ValueString.get(serviceName + "." + methodName + "()"));
        resultExpressions = new Expression[] { e };
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public Result getMetaData() {
        LocalResult result = new LocalResult(session, resultExpressions, 1);
        result.done();
        return result;
    }

    @Override
    public PreparedSQLStatement prepare() {
        for (int i = 0, size = expressions.size(); i < size; i++) {
            Expression e = expressions.get(i).optimize(session);
            expressions.set(i, e);
        }
        return this;
    }

    @Override
    public int update() {
        execute();
        return 0;
    }

    @Override
    public Result query(int maxRows) {
        setCurrentRowNumber(1);
        Value v = execute();
        Value[] row = { v };
        LocalResult result = new LocalResult(session, resultExpressions, 1);
        result.addRow(row);
        result.done();
        return result;
    }

    private Value execute() {
        Service service = Service.getService(session, session.getDatabase(),
                session.getCurrentSchemaName(), serviceName);
        session.getUser().checkRight(service, Right.EXECUTE);
        int size = expressions.size();
        Value[] methodArgs = new Value[size];
        for (int i = 0; i < size; i++) {
            methodArgs[i] = expressions.get(i).getValue(session);
        }
        return Service.execute(session, serviceName, methodName, methodArgs);
    }
}
