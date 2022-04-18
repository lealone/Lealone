/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.sql.Timestamp;
import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.orm.Model;

/**
 * Property for java sql Timestamp.
 */
public class PTimestamp<M extends Model<M>> extends PBaseDate<M, Timestamp> {

    private Timestamp value;

    public PTimestamp(String name, M model) {
        super(name, model);
    }

    private PTimestamp<M> P(M model) {
        return this.<PTimestamp<M>> getModelProperty(model);
    }

    public final M set(Timestamp value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueTimestamp.get(value));
        }
        return model;
    }

    public final Timestamp get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTimestamp();
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
        value = new Timestamp(((Number) v).longValue());
    }
}
