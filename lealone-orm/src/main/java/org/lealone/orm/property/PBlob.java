/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.sql.Blob;
import java.sql.SQLException;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.ReadonlyBlob;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBytes;
import org.lealone.db.value.ValueJavaObject;
import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;

public class PBlob<M extends Model<M>> extends ModelProperty<M> {

    private Blob value;

    public PBlob(String name, M model) {
        super(name, model);
    }

    private PBlob<M> P(M model) {
        return this.<PBlob<M>> getModelProperty(model);
    }

    public M set(Blob value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueJavaObject.getNoCopy(value, null));
        }
        return model;
    }

    public final Blob get() {
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBlob();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        if (value != null) {
            try {
                map.put(getName(), value.getBytes(0, (int) value.length()));
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        }
    }

    @Override
    protected void deserialize(Object v) {
        value = new ReadonlyBlob(ValueBytes.get((byte[]) v));
    }
}
