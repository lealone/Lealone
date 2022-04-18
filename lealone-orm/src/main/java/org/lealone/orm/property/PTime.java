/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.sql.Time;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueTime;
import org.lealone.orm.Model;

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
    protected Object encodeValue() {
        return value.getTime();
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTime();
    }

    @Override
    protected void deserialize(Object v) {
        value = new Time(((Number) v).longValue());
    }
}
