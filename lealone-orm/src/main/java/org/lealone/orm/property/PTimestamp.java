/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.sql.Timestamp;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.orm.Model;

/**
 * Property for java sql Timestamp.
 */
public class PTimestamp<M extends Model<M>> extends PBaseDate<M, Timestamp> {

    public PTimestamp(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Timestamp value) {
        return ValueTimestamp.get(value);
    }

    @Override
    protected Object encodeValue() {
        return value.getTime();
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTimestamp();
    }

    @Override
    protected void deserialize(Object v) {
        value = new Timestamp(((Number) v).longValue());
    }
}
