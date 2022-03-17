/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.json;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.service.JsonArrayDecoder;
import org.lealone.db.value.Value;

public class JsonArrayDecoderImpl implements JsonArrayDecoder {

    private JsonArray ja;

    @Override
    public void init(String json) {
        ja = new JsonArray(json);
    }

    @Override
    public Object getValue(int i, int type) {
        Object v;
        switch (type) {
        case Value.BOOLEAN:
            v = Boolean.valueOf(ja.getValue(i).toString());
            break;
        case Value.BYTE:
            v = Byte.valueOf(ja.getValue(i).toString());
            break;
        case Value.SHORT:
            v = Short.valueOf(ja.getValue(i).toString());
            break;
        case Value.INT:
            v = Integer.valueOf(ja.getValue(i).toString());
            break;
        case Value.LONG:
            v = Long.valueOf(ja.getValue(i).toString());
            break;
        case Value.DECIMAL:
            v = new java.math.BigDecimal(ja.getValue(i).toString());
            break;
        case Value.TIME:
            v = java.sql.Time.valueOf(ja.getValue(i).toString());
            break;
        case Value.DATE:
            v = java.sql.Date.valueOf(ja.getValue(i).toString());
            break;
        case Value.TIMESTAMP:
            v = java.sql.Timestamp.valueOf(ja.getValue(i).toString());
            break;
        case Value.BYTES:
            v = ja.getString(i).getBytes();
            break;
        case Value.UUID:
            v = java.util.UUID.fromString(ja.getValue(i).toString());
            break;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            v = ja.getString(i);
            break;
        case Value.BLOB:
            v = ja.getJsonObject(i).mapTo(java.sql.Blob.class);
            break;
        case Value.CLOB:
            v = ja.getJsonObject(i).mapTo(java.sql.Clob.class);
            break;
        case Value.DOUBLE:
            v = Double.valueOf(ja.getValue(i).toString());
            break;
        case Value.FLOAT:
            v = Float.valueOf(ja.getValue(i).toString());
            break;
        case Value.NULL:
            return null;
        case Value.JAVA_OBJECT:
            v = ja.getJsonObject(i);
            break;
        case Value.UNKNOWN:
            v = ja.getJsonObject(i);
            break;
        case Value.ARRAY:
            v = ja.getJsonObject(i).mapTo(java.sql.Array.class);
            break;
        case Value.RESULT_SET:
            v = ja.getJsonObject(i);
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return v;
    }
}
