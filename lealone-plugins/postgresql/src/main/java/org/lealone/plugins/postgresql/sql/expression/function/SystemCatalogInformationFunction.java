/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.sql.expression.function;

import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueStringFixed;
import org.lealone.plugins.postgresql.sql.PgAlias;
import org.lealone.sql.expression.function.FunctionInfo;

public class SystemCatalogInformationFunction extends PgFunction {

    public static final int PG_CHAR_TO_ENCODING = 0, PG_ENCODING_TO_CHAR = 1;

    public static void init() {
        // addFunctionNotDeterministic("PG_CHAR_TO_ENCODING", PG_CHAR_TO_ENCODING, 1, Value.STRING_FIXED);
        // addFunctionNotDeterministic("PG_ENCODING_TO_CHAR", PG_ENCODING_TO_CHAR, 1, Value.INT);
    }

    public SystemCatalogInformationFunction(Database database, FunctionInfo info) {
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
        case PG_CHAR_TO_ENCODING:
            result = ValueInt.get(PgAlias.getEncodingCode(v.getString()));
            break;
        case PG_ENCODING_TO_CHAR:
            result = ValueStringFixed.get(PgAlias.getEncodingName(v.getInt()));
            break;
        default:
            throw getUnsupportedException();
        }
        return result;
    }
}
