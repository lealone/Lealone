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
 *
 * @param <R> the root model bean type
 */
public class PDate<R> extends PBaseDate<R, Date> {

    private Date value;

    public PDate(String name, R root) {
        super(name, root);
    }

    private PDate<R> P(Model<?> model) {
        return this.<PDate<R>> getModelProperty(model);
    }

    public final R set(Date value) {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, ValueDate.get(value));
        }
        return root;
    }

    @Override
    public R set(Object value) {
        return set(Date.valueOf(value.toString()));
    }

    public final Date get() {
        Model<?> model = getModel();
        if (model != root) {
            return P(model).get();
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
