/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.function;

import com.lealone.db.Database;

//其他数据库可以继承这个类把自己的专有函数变成lealone的内置函数
public abstract class BuiltInFunctionExt extends BuiltInFunction {

    protected BuiltInFunctionExt(Database database, FunctionInfo info) {
        super(database, info);
    }
}
