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
import org.lealone.db.value.ValueJavaObject;
import org.lealone.orm.Model;

public class PBlob<M extends Model<M>> extends PBase<M, Blob> {

    public PBlob(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Blob value) {
        return ValueJavaObject.getNoCopy(value, null);
    }

    @Override
    protected Object encodeValue() {
        try {
            return value.getBytes(0, (int) value.length());
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBlob();
    }

    @Override
    protected void deserialize(Object v) {
        value = new ReadonlyBlob(ValueBytes.get((byte[]) v));
    }
}
