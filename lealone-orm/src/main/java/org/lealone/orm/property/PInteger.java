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
 */
public class PInteger<M extends Model<M>> extends PBaseNumber<M, Integer> {

    private int value;

    public PInteger(String name, M model) {
        super(name, model);
    }

    private PInteger<M> P(M model) {
        return this.<PInteger<M>> getModelProperty(model);
    }

    public final M set(int value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueInt.get(value));
        }
        return model;
    }

    public final int get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
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
