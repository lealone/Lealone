/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.sql.expression.function;

import com.lealone.db.Database;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueString;
import com.lealone.sql.expression.function.FunctionInfo;

public class PerformanceSchemaFunction extends MySQLFunction {

    public static final int FORMAT_BYTES = 110, FORMAT_PICO_TIME = 111, PS_CURRENT_THREAD_ID = 112,
            PS_THREAD_ID = 113;

    public static void init() {
        addFunction("FORMAT_BYTES", FORMAT_BYTES, 1, Value.STRING);
        addFunction("FORMAT_PICO_TIME", FORMAT_PICO_TIME, 1, Value.STRING);
        addFunction("PS_CURRENT_THREAD_ID", PS_CURRENT_THREAD_ID, 1, Value.LONG);
        addFunction("PS_THREAD_ID", PS_THREAD_ID, 1, Value.LONG);
    }

    public PerformanceSchemaFunction(Database database, FunctionInfo info) {
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
        case FORMAT_BYTES: {
            if (isNull(v))
                return ValueNull.INSTANCE;
            long v2 = v.getLong();
            String v3;
            if (v2 < 1024)
                v3 = v2 + " bytes";
            else if (v2 < 1024 * 1024)
                v3 = v2 / 1024 + " KiB";
            else if (v2 < 1024 * 1024 * 1024)
                v3 = v2 / 1024 / 1024 + " MiB";
            else if (v2 < 1024 * 1024 * 1024 * 1024)
                v3 = v2 / 1024 / 1024 / 1024 + " GiB";
            else if (v2 < 1024 * 1024 * 1024 * 1024 * 1024)
                v3 = v2 / 1024 / 1024 / 1024 / 1024 + " TiB";
            else if (v2 < 1024 * 1024 * 1024 * 1024 * 1024 * 1024)
                v3 = v2 / 1024 / 1024 / 1024 / 1024 / 1024 + " PiB";
            else
                v3 = v2 / 1024 / 1024 / 1024 / 1024 / 1024 / 1024 + " EiB";
            result = ValueString.get(v3);
            break;
        }
        case FORMAT_PICO_TIME:
            if (isNull(v))
                return ValueNull.INSTANCE;
            long v2 = v.getLong();
            String v3 = v2 + "";
            result = ValueString.get(v3);
            break;
        case PS_CURRENT_THREAD_ID:
        case PS_THREAD_ID:
            result = InformationFunction.getSchedulerId(session);
            break;
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    private boolean isNull(Value v) {
        return v == null || v.getType() == Value.NULL;
    }
}
