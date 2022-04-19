/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueString;
import org.lealone.orm.Model;

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
        Value[] array = new Value[values.length];
        for (int i = 0; i < values.length; i++) {
            array[i] = ValueString.get(values[i].toString());
        }
        return ValueArray.get(array);
    }

    @Override
    protected Object encodeValue() {
        return Arrays.asList(value);
    }

    @Override
    protected void deserialize(Object v) {
        if (v instanceof List)
            value = ((List<?>) v).toArray();
        else if (v instanceof Object[])
            value = (Object[]) v;
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
