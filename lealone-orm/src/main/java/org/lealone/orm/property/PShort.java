/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueShort;
import org.lealone.orm.Model;

/**
 * Short property.
 */
public class PShort<M extends Model<M>> extends PBaseNumber<M, Short> {

    public PShort(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Short value) {
        return ValueShort.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getShort();
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).shortValue();
    }
}
