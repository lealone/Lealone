/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueByte;
import org.lealone.orm.Model;

/**
 * Byte property.
 */
public class PByte<M extends Model<M>> extends PBaseNumber<M, Byte> {

    private byte value;

    public PByte(String name, M model) {
        super(name, model);
    }

    private PByte<M> P(M model) {
        return this.<PByte<M>> getModelProperty(model);
    }

    public final M set(byte value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueByte.get(value));
        }
        return model;
    }

    public final byte get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getByte();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        map.put(getName(), value);
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).byteValue();
    }
}
