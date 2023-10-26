/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.sql.expression.function;

import org.lealone.db.Database;
import org.lealone.sql.expression.function.FunctionInfo;

public class UserFunction extends MySQLFunction {

    protected UserFunction(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    protected void checkParameterCount(int len) {
    }
}
