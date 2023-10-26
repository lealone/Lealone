/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server;

import java.sql.Types;

import org.lealone.db.value.Value;

public class PgType {

    /**
     * The VARCHAR type.
     */
    public static final int PG_TYPE_VARCHAR = 1043;

    /**
     * The integer array type (for the column pg_index.indkey).
     */
    public static final int PG_TYPE_INT2VECTOR = 22;

    private static final int PG_TYPE_BOOL = 16;
    private static final int PG_TYPE_BYTEA = 17;
    private static final int PG_TYPE_BPCHAR = 1042;
    private static final int PG_TYPE_INT8 = 20;
    private static final int PG_TYPE_INT2 = 21;
    private static final int PG_TYPE_INT4 = 23;
    private static final int PG_TYPE_TEXT = 25;
    private static final int PG_TYPE_OID = 26;
    private static final int PG_TYPE_FLOAT4 = 700;
    private static final int PG_TYPE_FLOAT8 = 701;
    private static final int PG_TYPE_UNKNOWN = 705;
    private static final int PG_TYPE_TEXTARRAY = 1009;
    private static final int PG_TYPE_DATE = 1082;
    private static final int PG_TYPE_TIME = 1083;
    private static final int PG_TYPE_TIMESTAMP_NO_TMZONE = 1114;
    private static final int PG_TYPE_NUMERIC = 1700;

    /**
     * Convert the SQL type to a PostgreSQL type
     *
     * @param type the SQL type
     * @return the PostgreSQL type
     */
    public static int convertType(int type) {
        switch (type) {
        case Types.BOOLEAN:
            return PG_TYPE_BOOL;
        case Types.VARCHAR:
            return PG_TYPE_VARCHAR;
        case Types.CLOB:
            return PG_TYPE_TEXT;
        case Types.CHAR:
            return PG_TYPE_BPCHAR;
        case Types.SMALLINT:
            return PG_TYPE_INT2;
        case Types.INTEGER:
            return PG_TYPE_INT4;
        case Types.BIGINT:
            return PG_TYPE_INT8;
        case Types.DECIMAL:
            return PG_TYPE_NUMERIC;
        case Types.REAL:
            return PG_TYPE_FLOAT4;
        case Types.DOUBLE:
            return PG_TYPE_FLOAT8;
        case Types.TIME:
            return PG_TYPE_TIME;
        case Types.DATE:
            return PG_TYPE_DATE;
        case Types.TIMESTAMP:
            return PG_TYPE_TIMESTAMP_NO_TMZONE;
        case Types.VARBINARY:
            return PG_TYPE_BYTEA;
        case Types.BLOB:
            return PG_TYPE_OID;
        case Types.ARRAY:
            return PG_TYPE_TEXTARRAY;
        default:
            return PG_TYPE_UNKNOWN;
        }
    }

    public static int convertValueType(int type) {
        switch (type) {
        case Value.BOOLEAN:
            return PG_TYPE_BOOL;
        case Value.STRING:
            return PG_TYPE_VARCHAR;
        case Value.CLOB:
            return PG_TYPE_TEXT;
        case Value.STRING_FIXED:
            return PG_TYPE_BPCHAR;
        case Value.SHORT:
            return PG_TYPE_INT2;
        case Value.INT:
            return PG_TYPE_INT4;
        case Value.LONG:
            return PG_TYPE_INT8;
        case Value.DECIMAL:
            return PG_TYPE_NUMERIC;
        case Value.FLOAT:
            return PG_TYPE_FLOAT4;
        case Value.DOUBLE:
            return PG_TYPE_FLOAT8;
        case Value.TIME:
            return PG_TYPE_TIME;
        case Value.DATE:
            return PG_TYPE_DATE;
        case Value.TIMESTAMP:
            return PG_TYPE_TIMESTAMP_NO_TMZONE;
        case Value.BYTES:
            return PG_TYPE_BYTEA;
        case Value.BLOB:
            return PG_TYPE_OID;
        case Value.ARRAY:
            return PG_TYPE_TEXTARRAY;
        default:
            return PG_TYPE_UNKNOWN;
        }
    }
}
