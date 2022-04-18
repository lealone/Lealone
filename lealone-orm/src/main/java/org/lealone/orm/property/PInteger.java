/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.orm.Model;

/**
 * Integer property. 
 */
public class PInteger<M extends Model<M>> extends PBaseNumber<M, Integer> {

    public PInteger(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Integer value) {
        return ValueInt.get(value);
    }

    @Override
    protected void deserialize(Object v) {
        value = ((Number) v).intValue();
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getInt();
    }
}
