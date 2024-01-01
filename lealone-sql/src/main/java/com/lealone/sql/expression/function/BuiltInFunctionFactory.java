/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.function;

import com.lealone.db.Database;

public class BuiltInFunctionFactory implements FunctionFactory {

    public static final BuiltInFunctionFactory INSTANCE = new BuiltInFunctionFactory();

    public static void register() {
        Function.registerFunctionFactory(INSTANCE);
    }

    @Override
    public void init() {
        DateTimeFunction.init();
        NumericFunction.init();
        StringFunction.init();
        SystemFunction.init();
        TableFunction.init();
    }

    @Override
    public Function createFunction(Database database, FunctionInfo info) {
        if (info.type < StringFunction.ASCII)
            return new NumericFunction(database, info);
        if (info.type < DateTimeFunction.CURDATE)
            return new StringFunction(database, info);
        if (info.type < SystemFunction.DATABASE)
            return new DateTimeFunction(database, info);
        if (info.type < TableFunction.TABLE)
            return new SystemFunction(database, info);
        return new TableFunction(database, info);
    }
}
