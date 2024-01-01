/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.sql.expression.function;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.Database;
import com.lealone.sql.expression.function.Function;
import com.lealone.sql.expression.function.FunctionFactory;
import com.lealone.sql.expression.function.FunctionInfo;

public class MySQLFunctionFactory implements FunctionFactory {

    public static final MySQLFunctionFactory INSTANCE = new MySQLFunctionFactory();

    public static void register() {
        Function.registerFunctionFactory(INSTANCE);
    }

    @Override
    public void init() {
        InformationFunction.init();
        BitFunction.init();
        PerformanceSchemaFunction.init();
    }

    @Override
    public Function createFunction(Database database, FunctionInfo info) {
        if (info.type < 0)
            return new UserFunction(database, info);
        else if (info.type < 100)
            return new InformationFunction(database, info);
        else if (info.type < 110)
            return new BitFunction(database, info);
        else if (info.type < 120)
            return new PerformanceSchemaFunction(database, info);
        throw DbException.getInternalError();
    }
}
