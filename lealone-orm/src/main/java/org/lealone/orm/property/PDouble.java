/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDouble;
import org.lealone.orm.Model;

/**
 * Double property.
 *
 * @param <R> the root model bean type
 */
public class PDouble<R> extends PBaseNumber<R, Double> {

    private double value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PDouble(String name, R root) {
        super(name, root);
    }

    private PDouble<R> P(Model<?> model) {
        return this.<PDouble<R>> getModelProperty(model);
    }

    public final R set(double value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueDouble.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Double.valueOf(value.toString()).doubleValue());
    }

    public final double get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getDouble();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        map.put(getName(), value);
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).doubleValue();
    }
}
