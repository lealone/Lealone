/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.sql.Date;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueDate;
import com.lealone.orm.Model;

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
    protected void deserialize(Value v) {
        value = v.getDate();
    }

    @Override
    protected Object encode() {
        return value.getTime();
    }

    @Override
    protected Date decode(Object v) {
        return new Date(((Number) v).longValue());
    }
}
