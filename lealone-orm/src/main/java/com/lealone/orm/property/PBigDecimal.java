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
import com.lealone.orm.format.BigDecimalFormat;
import com.lealone.orm.format.JsonFormat;

/**
 * BigDecimal property.
 */
public class PBigDecimal<M extends Model<M>> extends PBaseNumber<M, BigDecimal> {

    public PBigDecimal(String name, M model) {
        super(name, model);
    }

    @Override
    protected BigDecimalFormat getValueFormat(JsonFormat format) {
        return format.getBigDecimalFormat();
    }

    @Override
    protected Value createValue(BigDecimal value) {
        return ValueDecimal.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBigDecimal();
    }
}
