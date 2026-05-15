/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueInt;
import com.lealone.orm.Model;

/**
 * Integer property. 
 */
public class PInteger<M extends Model<M>> extends PBaseNumber<M, Integer> {

    public PInteger(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Integer value) {
        return ValueInt.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getInt();
    }

    @Override
    protected Integer decode(Object v) {
        return ((Number) v).intValue();
    }
}
