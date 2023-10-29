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
import org.lealone.db.value.ValueStringFixed;
import org.lealone.plugins.mysql.server.MySQLServer;
import org.lealone.sql.expression.function.FunctionInfo;

public class InformationFunction extends MySQLFunction {

    public static final int CONNECTION_ID = 0, VERSION = 1;

    public static void init() {
        addFunction("CONNECTION_ID", CONNECTION_ID, 0, Value.LONG);
        addFunction("VERSION", VERSION, 0, Value.STRING);
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
        return ValueLong.get(
                session.getTransactionHandler() != null ? session.getTransactionHandler().getHandlerId()
                        : 0);
    }
}
