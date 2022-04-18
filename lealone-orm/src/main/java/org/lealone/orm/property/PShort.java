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
 *
 * @param <R> the root model bean type
 */
public class PShort<R> extends PBaseNumber<R, Short> {

    private short value;

    public PShort(String name, R root) {
        super(name, root);
    }

    private PShort<R> P(Model<?> model) {
        return this.<PShort<R>> getModelProperty(model);
    }

    public final R set(short value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueShort.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Short.valueOf(value.toString()).shortValue());
    }

    public final short get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
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
