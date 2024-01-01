/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.sql.expression.function;

import com.lealone.db.Database;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueStringFixed;
import com.lealone.plugins.mysql.server.MySQLServer;
import com.lealone.sql.expression.function.FunctionInfo;

public class InformationFunction extends MySQLFunction {

    public static final int CONNECTION_ID = 0, VERSION = 1;

    public static void init() {
        addFunction("CONNECTION_ID", CONNECTION_ID, 0, Value.LONG);
        // 使用Lealone自带的
        // addFunction("VERSION", VERSION, 0, Value.STRING);
    }

    public InformationFunction(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    protected void checkParameterCount(int len) {
        // 有可变参数才需要实现
    }

    @Override
    protected Value getValue0(ServerSession session) {
        Value result;
        switch (info.type) {
        case CONNECTION_ID:
            // 返回的是调度器的id
            result = getSchedulerId(session);
            break;
        case VERSION:
            result = ValueStringFixed.get(MySQLServer.SERVER_VERSION);
            break;
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    public static ValueLong getSchedulerId(ServerSession session) {
        return ValueLong.get(session.getScheduler() != null ? session.getScheduler().getId() : 0);
    }
}
