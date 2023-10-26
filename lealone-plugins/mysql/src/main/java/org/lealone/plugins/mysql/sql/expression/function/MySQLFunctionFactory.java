/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.sql.expression.function;

import org.lealone.db.Database;
import org.lealone.sql.expression.function.Function;
import org.lealone.sql.expression.function.FunctionFactory;
import org.lealone.sql.expression.function.FunctionInfo;

public class MySQLFunctionFactory implements FunctionFactory {

    public static final MySQLFunctionFactory INSTANCE = new MySQLFunctionFactory();

    public static void register() {
        Function.registerFunctionFactory(INSTANCE);
    }

    @Override
    public void init() {
        InformationFunction.init();
    }

    @Override
    public Function createFunction(Database database, FunctionInfo info) {
        if (info.type < 0)
            return new UserFunction(database, info);
        else
            return new InformationFunction(database, info);
    }
}
