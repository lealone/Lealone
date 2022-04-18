/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueShort;
import org.lealone.orm.Model;

/**
 * Short property.
 */
public class PShort<M extends Model<M>> extends PBaseNumber<M, Short> {

    private short value;

    public PShort(String name, M model) {
        super(name, model);
    }

    private PShort<M> P(M model) {
        return this.<PShort<M>> getModelProperty(model);
    }

    public final M set(short value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueShort.get(value));
        }
        return model;
    }

    @Override
    public M set(Object value) {
        return set(Short.valueOf(value.toString()).shortValue());
    }

    public final short get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getShort();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        map.put(getName(), value);
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).shortValue();
    }
}
