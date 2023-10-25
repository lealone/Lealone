/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.function;

import org.lealone.db.Database;

public class BuiltInFunctionFactory implements FunctionFactory {

    public static final BuiltInFunctionFactory INSTANCE = new BuiltInFunctionFactory();

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
