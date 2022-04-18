/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.sql.Date;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDate;
import org.lealone.orm.Model;

/**
 * Java sql date property. 
 */
public class PDate<M extends Model<M>> extends PBaseDate<M, Date> {

    public PDate(String name, M model) {
        super(name, model);
    }

    public M set(String value) {
        return set(Date.valueOf(value.toString()));
    }

    @Override
    protected Value createValue(Date value) {
        return ValueDate.get(value);
    }

    @Override
    protected Object encodeValue() {
        return value.getTime();
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getDate();
    }

    @Override
    protected void deserialize(Object v) {
        value = new Date(((Number) v).longValue());
    }
}
