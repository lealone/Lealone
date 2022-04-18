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
 *
 * @param <R> the root model bean type
 */
public class PTime<R> extends PBaseNumber<R, Time> {

    private Time value;

    public PTime(String name, R root) {
        super(name, root);
    }

    private PTime<R> P(Model<?> model) {
        return this.<PTime<R>> getModelProperty(model);
    }

    public final R set(Time value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueTime.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Time.valueOf(value.toString()));
    }

    public final Time get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
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
