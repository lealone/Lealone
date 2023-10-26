/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.function;

import org.lealone.db.Database;

public interface FunctionFactory {

    void init();

    Function createFunction(Database database, FunctionInfo info);

}
