/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.sql.Date;
import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDate;
import org.lealone.orm.Model;

/**
 * Java sql date property. 
 */
public class PDate<M extends Model<M>> extends PBaseDate<M, Date> {

    private Date value;

    public PDate(String name, M model) {
        super(name, model);
    }

    private PDate<M> P(M model) {
        return this.<PDate<M>> getModelProperty(model);
    }

    public final M set(Date value) {
        M m = getModel();
        if (m != model) {
            return P(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueDate.get(value));
        }
        return model;
    }

    public final Date get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return value;
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getDate();
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        if (value != null)
            map.put(getName(), value.getTime());
        else
            map.put(getName(), 0);
    }

    @Override
    protected void deserialize(Object v) {
        value = new Date(((Number) v).longValue());
    }
}
