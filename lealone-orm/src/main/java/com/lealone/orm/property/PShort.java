/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueShort;
import com.lealone.orm.Model;
import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.format.ShortFormat;

/**
 * Short property.
 */
public class PShort<M extends Model<M>> extends PBaseNumber<M, Short> {

    public PShort(String name, M model) {
        super(name, model);
    }

    @Override
    protected ShortFormat getValueFormat(JsonFormat format) {
        return format.getShortFormat();
    }

    @Override
    protected Value createValue(Short value) {
        return ValueShort.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getShort();
    }
}
