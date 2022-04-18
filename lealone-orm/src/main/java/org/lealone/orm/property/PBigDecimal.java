/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.math.BigDecimal;
import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDecimal;
import org.lealone.orm.Model;

/**
 * BigDecimal property.
 */
public class PBigDecimal<M extends Model<M>> extends PBaseNumber<M, BigDecimal> {

    private BigDecimal value;

    public PBigDecimal(String name, M model) {
        super(name, model);
    }

    private PBigDecimal<M> P(M model) {
        return this.<PBigDecimal<M>> getModelProperty(model);
    }

    public final M set(BigDecimal value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueDecimal.get(value));
        }
        return model;
    }

    public final BigDecimal get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBigDecimal();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        if (value != null)
            map.put(getName(), value.toString());
    }

    @Override
    protected void deserialize(Object v) {
        value = new BigDecimal(value.toString());
    }
}
