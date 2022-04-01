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
 *
 * @param <R> the root model bean type
 */
public class PTimestamp<R> extends PBaseDate<R, Timestamp> {

    private Timestamp value;

    /**
     * Construct with a property name and root instance.
     *
     * @param name property name
     * @param root the root model bean instance
     */
    public PTimestamp(String name, R root) {
        super(name, root);
    }

    private PTimestamp<R> P(Model<?> model) {
        return this.<PTimestamp<R>> getModelProperty(model);
    }

    public final R set(Timestamp value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueTimestamp.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Timestamp.valueOf(value.toString()));
    }

    public final Timestamp get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
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
