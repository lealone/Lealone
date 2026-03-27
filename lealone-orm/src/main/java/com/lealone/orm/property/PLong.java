/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLong;
import com.lealone.orm.Model;
import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.format.LongFormat;

/**
 * Long property.
 */
public class PLong<M extends Model<M>> extends PBaseNumber<M, Long> {

    public PLong(String name, M model) {
        super(name, model);
    }

    @Override
    protected LongFormat getValueFormat(JsonFormat format) {
        return format.getLongFormat();
    }

    // 支持int，避免总是加L后缀
    public final M set(long value) {
        return super.set(value);
    }

    @Override
    protected Value createValue(Long value) {
        return ValueLong.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getLong();
    }
}
