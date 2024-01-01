/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.postgresql.sql.expression.function;

import com.lealone.db.Database;
import com.lealone.sql.expression.function.BuiltInFunctionExt;
import com.lealone.sql.expression.function.Function;
import com.lealone.sql.expression.function.FunctionInfo;

public abstract class PgFunction extends BuiltInFunctionExt {

    protected static FunctionInfo addFunctionNotDeterministic(String name, int type, int parameterCount,
            int dataType) {
        FunctionInfo info = Function.addFunctionNotDeterministic(name, type, parameterCount, dataType);
        info.factory = PgFunctionFactory.INSTANCE;
        return info;
    }

    protected static FunctionInfo addFunction(String name, int type, int parameterCount, int dataType) {
        FunctionInfo info = Function.addFunction(name, type, parameterCount, dataType);
        info.factory = PgFunctionFactory.INSTANCE;
        return info;
    }

    protected PgFunction(Database database, FunctionInfo info) {
        super(database, info);
    }
}
