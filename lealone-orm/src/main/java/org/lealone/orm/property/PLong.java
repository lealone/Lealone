/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueLong;
import org.lealone.orm.Model;

/**
 * Long property.
 */
public class PLong<M extends Model<M>> extends PBaseNumber<M, Long> {

    public PLong(String name, M model) {
        super(name, model);
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
    protected void deserialize(Object v) {
        value = ((Number) v).longValue();
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getLong();
    }
}
