/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueDouble;
import com.lealone.orm.Model;

/**
 * Double property. 
 */
public class PDouble<M extends Model<M>> extends PBaseNumber<M, Double> {

    public PDouble(String name, M model) {
        super(name, model);
    }

    // 支持Float，避免总是加D后缀
    public final M set(double value) {
        return super.set(value);
    }

    @Override
    protected Value createValue(Double value) {
        return ValueDouble.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getDouble();
    }

    @Override
    protected Double decode(Object v) {
        return ((Number) v).doubleValue();
    }
}
