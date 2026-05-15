/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.orm.Model;

/**
 * Array property.
 */
public class PArray<M extends Model<M>> extends PBase<M, Object[]> {

    public PArray(String name, M model) {
        super(name, model);
    }

    public M set(Array value) {
        try {
            return set((Object[]) value.getArray());
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected Value createValue(Object[] values) {
        return DataType.convertToValue(values, Value.ARRAY);
    }

    @Override
    protected void deserialize(Value v) {
        if (v instanceof ValueArray) {
            ValueArray array = (ValueArray) v;
            Value[] list = array.getList();
            int length = list.length;
            Object[] values = new Object[length];
            for (int i = 0; i < length; i++) {
                values[i] = list[i].getObject();
            }
            this.value = values;
        }
    }

    @Override
    protected Object encode() {
        return Arrays.asList(value);
    }

    @Override
    protected Object[] decode(Object v) {
        if (v instanceof List)
            return ((List<?>) v).toArray();
        else if (v instanceof Object[])
            return (Object[]) v;
        else
            return new Object[] { v };
    }

    /**
     * ARRAY contains the values.
     *
     * @param values The values that should be contained in the array
     */
    @SafeVarargs
    public final M contains(Object... values) {
        return expr().arrayContains(name, values);
    }

    /**
     * ARRAY does not contain the values.
     *
     * @param values The values that should not be contained in the array
     */
    @SafeVarargs
    public final M notContains(Object... values) {
        return expr().arrayNotContains(name, values);
    }

    /**
     * ARRAY is empty.
     */
    public M isEmpty() {
        return expr().arrayIsEmpty(name);
    }

    /**
     * ARRAY is not empty.
     */
    public M isNotEmpty() {
        return expr().arrayIsNotEmpty(name);
    }
}
