/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.sql.Time;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueTime;
import com.lealone.orm.Model;

/**
 * Time property.
 */
public class PTime<M extends Model<M>> extends PBaseNumber<M, Time> {

    public PTime(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Time value) {
        return ValueTime.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTime();
    }

    @Override
    protected Object encode() {
        return value.getTime();
    }

    @Override
    protected Time decode(Object v) {
        return new Time(((Number) v).longValue());
    }
}
