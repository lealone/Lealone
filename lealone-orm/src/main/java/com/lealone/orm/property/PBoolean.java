/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueBoolean;
import com.lealone.orm.Model;
import com.lealone.orm.format.BooleanFormat;
import com.lealone.orm.format.JsonFormat;

/**
 * Boolean property.
 */
public class PBoolean<M extends Model<M>> extends PBaseValueEqual<M, Boolean> {

    public PBoolean(String name, M model) {
        super(name, model);
    }

    @Override
    protected BooleanFormat getValueFormat(JsonFormat format) {
        return format.getBooleanFormat();
    }

    @Override
    protected Value createValue(Boolean value) {
        return ValueBoolean.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBoolean();
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
