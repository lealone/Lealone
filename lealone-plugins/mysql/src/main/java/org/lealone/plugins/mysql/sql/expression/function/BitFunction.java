/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.sql.expression.function;

import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.sql.expression.function.FunctionInfo;

public class BitFunction extends MySQLFunction {

    public static final int BIT_COUNT = 100;

    public static void init() {
        addFunction("BIT_COUNT", BIT_COUNT, 1, Value.LONG);
        // 跟lealone的聚合函数冲突了
        // addFunction("BIT_AND", Function.getFunctionInfo("BITAND"));
        // addFunction("BIT_OR", Function.getFunctionInfo("BITOR"));
        // addFunction("BIT_XOR", Function.getFunctionInfo("BITXOR"));
    }

    public BitFunction(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    protected void checkParameterCount(int len) {
        // 有可变参数才需要实现
    }

    @Override
    protected Value getValue1(ServerSession session, Value v) {
        Value result;
        switch (info.type) {
        case BIT_COUNT:
            result = ValueLong.get(Long.bitCount(v.getLong()));
            break;
        default:
            throw getUnsupportedException();
        }
        return result;
    }
}
