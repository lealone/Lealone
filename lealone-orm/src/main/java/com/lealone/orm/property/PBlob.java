/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.sql.Blob;
import java.sql.SQLException;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.value.ReadonlyBlob;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueBytes;
import com.lealone.orm.Model;
import com.lealone.orm.json.Json;

public class PBlob<M extends Model<M>> extends PBase<M, Blob> {

    public PBlob(String name, M model) {
        super(name, model);
    }

    private static byte[] getBytes(Blob value) {
        try {
            return value.getBytes(1, (int) value.length());
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected Value createValue(Blob value) {
        return ValueBytes.getNoCopy(getBytes(value));
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBlob();
    }

    @Override
    protected Object encode() {
        return Json.BASE64_ENCODER.encodeToString(getBytes(value));
    }

    @Override
    protected Blob decode(Object v) {
        byte[] bytes;
        if (v instanceof byte[])
            bytes = (byte[]) v;
        else
            bytes = Json.BASE64_DECODER.decode(v.toString());
        return new ReadonlyBlob(ValueBytes.get(bytes));
    }
}
