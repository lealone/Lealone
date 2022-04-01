/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.orm.Model;

/**
 * Integer property.
 *
 * @param <R> the root model bean type
 */
public class PInteger<R> extends PBaseNumber<R, Integer> {

    private int value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PInteger(String name, R root) {
        super(name, root);
    }

    private PInteger<R> P(Model<?> model) {
        return this.<PInteger<R>> getModelProperty(model);
    }

    public final R set(int value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueInt.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Integer.valueOf(value.toString()).intValue());
    }

    public final int get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
        }
        return value;
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        map.put(getName(), value);
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).intValue();
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getInt();
    }
}
