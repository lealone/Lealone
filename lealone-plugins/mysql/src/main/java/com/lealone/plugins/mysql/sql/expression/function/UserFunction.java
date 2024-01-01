/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.sql.expression.function;

import com.lealone.db.Database;
import com.lealone.sql.expression.function.FunctionInfo;

public class UserFunction extends MySQLFunction {

    protected UserFunction(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    protected void checkParameterCount(int len) {
    }
}
