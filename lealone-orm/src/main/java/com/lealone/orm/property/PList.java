/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.util.List;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueList;
import com.lealone.orm.Model;
import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.format.ListFormat;

/**
 * List property.
 */
public class PList<M extends Model<M>, E> extends PBase<M, List<E>> {

    public PList(String name, M model) {
        super(name, model);
    }

    @Override
    protected ListFormat<E> getValueFormat(JsonFormat format) {
        return format.getListFormat();
    }

    @Override
    protected Value createValue(List<E> values) {
        return ValueList.get(values);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void deserialize(Value v) {
        if (v instanceof ValueList) {
            this.value = (List<E>) v.getObject();
        }
    }
}
