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
 */
public class PDouble<M extends Model<M>> extends PBaseNumber<M, Double> {

    private double value;

    public PDouble(String name, M model) {
        super(name, model);
    }

    private PDouble<M> P(M model) {
        return this.<PDouble<M>> getModelProperty(model);
    }

    public final M set(double value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueDouble.get(value));
        }
        return model;
    }

    @Override
    public M set(Object value) {
        return set(Double.valueOf(value.toString()).doubleValue());
    }

    public final double get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
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
