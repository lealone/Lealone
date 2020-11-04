/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.sql.dml;

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
        int size = expressions.size();
        Value[] methodArgs = new Value[size];
        for (int i = 0; i < size; i++) {
            methodArgs[i] = expressions.get(i).getValue(session);
        }
        return Service.execute(session, serviceName, methodName, methodArgs);
    }
}
