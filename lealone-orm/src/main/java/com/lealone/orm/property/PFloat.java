/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueFloat;
import com.lealone.orm.Model;
import com.lealone.orm.format.FloatFormat;
import com.lealone.orm.format.JsonFormat;

/**
 * Float property. 
 */
public class PFloat<M extends Model<M>> extends PBaseNumber<M, Float> {

    public PFloat(String name, M model) {
        super(name, model);
    }

    @Override
    protected FloatFormat getValueFormat(JsonFormat format) {
        return format.getFloatFormat();
    }

    @Override
    protected Value createValue(Float value) {
        return ValueFloat.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getFloat();
    }
}
