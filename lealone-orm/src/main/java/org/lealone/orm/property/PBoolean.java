/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;
import org.lealone.orm.Model;

/**
 * Boolean property.
 */
public class PBoolean<M extends Model<M>> extends PBaseValueEqual<M, Boolean> {

    public PBoolean(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Boolean value) {
        return ValueBoolean.get(value);
    }

    @Override
    protected Object encodeValue() {
        return value ? 1 : 0;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBoolean();
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).byteValue() != 0;
    }

    /**
     * Is true.
     *
     * @return the model instance
     */
    public M isTrue() {
        return expr().eq(name, Boolean.TRUE);
    }

    /**
     * Is false.
     *
     * @return the model instance
     */
    public M isFalse() {
        return expr().eq(name, Boolean.FALSE);
    }

    /**
     * Is true or false based on the bind value.
     *
     * @param value the equal to bind value
     *
     * @return the model instance
     */
    public M is(boolean value) {
        return expr().eq(name, value);
    }

    /**
     * Is true or false based on the bind value.
     *
     * @param value the equal to bind value
     *
     * @return the model instance
     */
    public M eq(boolean value) {
        return expr().eq(name, value);
    }
}
