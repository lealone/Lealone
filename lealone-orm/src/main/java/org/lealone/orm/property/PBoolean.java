/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.orm.Model;

/**
 * Boolean property.
 */
public class PBoolean<M extends Model<M>> extends PBaseValueEqual<M, Boolean> {

    private boolean value;

    public PBoolean(String name, M model) {
        super(name, model);
    }

    private PBoolean<M> P(M model) {
        return this.<PBoolean<M>> getModelProperty(model);
    }

    /**
     * Is true.
     *
     * @return the model bean instance
     */
    public M isTrue() {
        return expr().eq(name, Boolean.TRUE);
    }

    /**
     * Is false.
     *
     * @return the model bean instance
     */
    public M isFalse() {
        return expr().eq(name, Boolean.FALSE);
    }

    /**
     * Is true or false based on the bind value.
     *
     * @param value the equal to bind value
     *
     * @return the model bean instance
     */
    public M is(boolean value) {
        return expr().eq(name, value);
    }

    /**
     * Is true or false based on the bind value.
     *
     * @param value the equal to bind value
     *
     * @return the model bean instance
     */
    public M eq(boolean value) {
        return expr().eq(name, value);
    }

    public final M set(boolean value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueBoolean.get(value));
        }
        return model;
    }

    public final boolean get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBoolean();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        map.put(getName(), value ? 1 : 0);
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).byteValue() != 0;
    }
}
