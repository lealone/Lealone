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
 *
 * @param <R> the root model bean type
 */
public class PFloat<R> extends PBaseNumber<R, Float> {

    private float value;

    public PFloat(String name, R root) {
        super(name, root);
    }

    private PFloat<R> P(Model<?> model) {
        return this.<PFloat<R>> getModelProperty(model);
    }

    public final R set(float value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueFloat.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Float.valueOf(value.toString()).floatValue());
    }

    public final float get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
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
