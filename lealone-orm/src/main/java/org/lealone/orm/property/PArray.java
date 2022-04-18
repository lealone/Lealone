/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm.property;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueString;
import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;

/**
 * Array property with E as the element type.
 */
public class PArray<M extends Model<M>> extends ModelProperty<M> {

    private Object[] values;

    public PArray(String name, M model) {
        super(name, model);
    }

    private PArray<M> P(M model) {
        return this.<PArray<M>> getModelProperty(model);
    }

    public final M set(Object[] values) {
        M m = getModel();
        if (m != model) {
            return P(m).set(values);
        }
        if (!areEqual(this.values, values)) {
            this.values = values;
            Value[] array = new Value[values.length];
            for (int i = 0; i < values.length; i++) {
                array[i] = ValueString.get(values[i].toString());
            }
            expr().set(name, ValueArray.get(array));
        }
        return model;
    }

    public final Object[] get() {
        M m = getModel();
        if (m != model) {
            return P(m).get();
        }
        return values;
    }

    @Override
    protected void serialize(Map<String, Object> map) {
        if (values != null)
            map.put(getName(), Arrays.asList(values));
    }

    @Override
    protected void deserialize(Object v) {
        if (v instanceof List)
            values = ((List<?>) v).toArray();
        else if (v instanceof Object[])
            values = (Object[]) v;
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
            this.values = values;
        }
    }

    /**
     * ARRAY contains the values.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.contains("4321")
     *    .findList();
     *
     * }</pre>
     *
     * @param values The values that should be contained in the array
     */
    @SafeVarargs
    public final M contains(Object... values) {
        M m = getModel();
        if (m != model) {
            return P(m).contains(values);
        }
        expr().arrayContains(name, values);
        return model;
    }

    /**
     * ARRAY does not contain the values.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.notContains("4321")
     *    .findList();
     *
     * }</pre>
     *
     * @param values The values that should not be contained in the array
     */
    @SafeVarargs
    public final M notContains(Object... values) {
        M m = getModel();
        if (m != model) {
            return P(m).notContains(values);
        }
        expr().arrayNotContains(name, values);
        return model;
    }

    /**
     * ARRAY is empty.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.isEmpty()
     *    .findList();
     *
     * }</pre>
     */
    public M isEmpty() {
        M m = getModel();
        if (m != model) {
            return P(m).isEmpty();
        }
        expr().arrayIsEmpty(name);
        return model;
    }

    /**
     * ARRAY is not empty.
     * <p>
     * <pre>{@code
     *
     *   new QContact()
     *    .phoneNumbers.isNotEmpty()
     *    .findList();
     *
     * }</pre>
     */
    public M isNotEmpty() {
        M m = getModel();
        if (m != model) {
            return P(m).isNotEmpty();
        }
        expr().arrayIsNotEmpty(name);
        return model;
    }
}
