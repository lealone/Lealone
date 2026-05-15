/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.math.BigDecimal;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueDecimal;
import com.lealone.orm.Model;

/**
 * BigDecimal property.
 */
public class PBigDecimal<M extends Model<M>> extends PBaseNumber<M, BigDecimal> {

    public PBigDecimal(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(BigDecimal value) {
        return ValueDecimal.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBigDecimal();
    }

    @Override
    protected Object encode() {
        return value.toString();
    }

    @Override
    protected BigDecimal decode(Object v) {
        return new BigDecimal(v.toString());
    }
}
