/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.math.BigDecimal;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDecimal;
import org.lealone.orm.Model;

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
    protected Object encodeValue() {
        return value.toString();
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBigDecimal();
    }

    @Override
    protected void deserialize(Object v) {
        value = new BigDecimal(value.toString());
    }
}
