/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.sql.expression.function;

import org.lealone.db.Database;
import org.lealone.sql.expression.function.BuiltInFunctionExt;
import org.lealone.sql.expression.function.Function;
import org.lealone.sql.expression.function.FunctionInfo;

public abstract class MySQLFunction extends BuiltInFunctionExt {

    public static FunctionInfo addFunctionNotDeterministic(String name, int type, int parameterCount,
            int dataType) {
        FunctionInfo info = Function.addFunctionNotDeterministic(name, type, parameterCount, dataType);
        info.factory = MySQLFunctionFactory.INSTANCE;
        return info;
    }

    public static FunctionInfo addFunction(String name, int type, int parameterCount, int dataType) {
        FunctionInfo info = Function.addFunction(name, type, parameterCount, dataType);
        info.factory = MySQLFunctionFactory.INSTANCE;
        return info;
    }

    public static FunctionInfo addFunction(String name, int type, int parameterCount, int dataType,
            boolean deterministic) {
        return deterministic ? addFunction(name, type, parameterCount, dataType)
                : addFunctionNotDeterministic(name, type, parameterCount, dataType);
    }

    public MySQLFunction(Database database, FunctionInfo info) {
        super(database, info);
    }
}
