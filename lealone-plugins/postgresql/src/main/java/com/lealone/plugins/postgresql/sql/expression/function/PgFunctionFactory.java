/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.postgresql.sql.expression.function;

import com.lealone.db.Database;
import com.lealone.sql.expression.function.Function;
import com.lealone.sql.expression.function.FunctionFactory;
import com.lealone.sql.expression.function.FunctionInfo;

public class PgFunctionFactory implements FunctionFactory {

    public static final PgFunctionFactory INSTANCE = new PgFunctionFactory();

    public static void register() {
        Function.registerFunctionFactory(INSTANCE);
    }

    @Override
    public void init() {
        SystemCatalogInformationFunction.init();
    }

    @Override
    public Function createFunction(Database database, FunctionInfo info) {
        return new SystemCatalogInformationFunction(database, info);
    }
}
