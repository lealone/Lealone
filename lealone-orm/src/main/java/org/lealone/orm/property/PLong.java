/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.orm.Model;

/**
 * Long property.
 */
public class PLong<M extends Model<M>> extends PBaseNumber<M, Long> {

    private long value;

    public PLong(String name, M model) {
        super(name, model);
    }

    private PLong<M> P(M model) {
        return this.<PLong<M>> getModelProperty(model);
    }

    public M set(long value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueLong.get(value));
        }
        return model;
    }

    @Override
    public M set(Object value) {
        return set(Long.valueOf(value.toString()).longValue());
    }

    public final long get() {
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
        value = ((Number) v).longValue();
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getLong();
    }
}
