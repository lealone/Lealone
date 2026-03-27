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
import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.format.TimeFormat;

/**
 * Time property.
 */
public class PTime<M extends Model<M>> extends PBaseNumber<M, Time> {

    public PTime(String name, M model) {
        super(name, model);
    }

    @Override
    protected TimeFormat getValueFormat(JsonFormat format) {
        return format.getTimeFormat();
    }

    @Override
    protected Value createValue(Time value) {
        return ValueTime.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTime();
    }
}
