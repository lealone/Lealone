/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueFloat;
import org.lealone.orm.Model;

/**
 * Float property. 
 */
public class PFloat<M extends Model<M>> extends PBaseNumber<M, Float> {

    private float value;

    public PFloat(String name, M model) {
        super(name, model);
    }

    private PFloat<M> P(M model) {
        return this.<PFloat<M>> getModelProperty(model);
    }

    public final M set(float value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueFloat.get(value));
        }
        return model;
    }

    @Override
    public M set(Object value) {
        return set(Float.valueOf(value.toString()).floatValue());
    }

    public final float get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getFloat();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        map.put(getName(), value);
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).floatValue();
    }
}
