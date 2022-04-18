/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.sql.Time;
import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueTime;
import org.lealone.orm.Model;

/**
 * Time property.
 */
public class PTime<M extends Model<M>> extends PBaseNumber<M, Time> {

    private Time value;

    public PTime(String name, M model) {
        super(name, model);
    }

    private PTime<M> P(M model) {
        return this.<PTime<M>> getModelProperty(model);
    }

    public final M set(Time value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueTime.get(value));
        }
        return model;
    }

    @Override
    public M set(Object value) {
        return set(Time.valueOf(value.toString()));
    }

    public final Time get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTime();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        if (value != null)
            map.put(getName(), value.getTime());
        else
            map.put(getName(), 0);
    }

    @Override
    protected void deserialize(Object v) {
        value = new Time(((Number) v).longValue());
    }
}
