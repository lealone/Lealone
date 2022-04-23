/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.sql.Blob;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.ReadonlyBlob;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBytes;
import org.lealone.orm.Model;
import org.lealone.orm.json.Json;

public class PBlob<M extends Model<M>> extends PBase<M, Blob> {

    public PBlob(String name, M model) {
        super(name, model);
    }

    private byte[] getBytes() {
        try {
            return value.getBytes(1, (int) value.length());
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected Value createValue(Blob value) {
        return ValueBytes.getNoCopy(getBytes());
    }

    @Override
    protected Object encodeValue() {
        return Json.BASE64_ENCODER.encodeToString(getBytes());
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBlob();
    }

    @Override
    protected void deserialize(Object v) {
        byte[] bytes;
        if (v instanceof byte[])
            bytes = (byte[]) v;
        else
            bytes = Json.BASE64_DECODER.decode(v.toString());
        value = new ReadonlyBlob(ValueBytes.get(bytes));
    }
}
