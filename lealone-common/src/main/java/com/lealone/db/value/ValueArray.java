/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.value;

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.MathUtils;
import com.lealone.common.util.StatementBuilder;
import com.lealone.db.Constants;

/**
 * Implementation of the ARRAY data type.
 */
public class ValueArray extends Value {

    private final Class<?> componentType;
    private final Value[] values;
    private int hash;

    private ValueArray(Class<?> componentType, Value[] list) {
        this.componentType = componentType;
        this.values = list;
    }

    private ValueArray(Value[] list) {
        this(Object.class, list);
    }

    /**
     * Get or create a array value for the given value array.
     * Do not clone the data.
     *
     * @param list the value array
     * @return the value
     */
    public static ValueArray get(Value[] list) {
        return new ValueArray(list);
    }

    public static ValueArray get(java.sql.Array array) {
        Object[] objArray;
        try {
            objArray = (Object[]) array.getArray();
            int size = objArray.length;
            Value[] values = new Value[size];
            for (int i = 0; i < size; i++) {
                values[i] = ValueString.get(objArray[i].toString());
            }
            return new ValueArray(values);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Get or create a array value for the given value array.
     * Do not clone the data.
     *
     * @param componentType the array class (null for Object[])
     * @param list the value array
     * @return the value
     */
    public static ValueArray get(Class<?> componentType, Value[] list) {
        return new ValueArray(componentType, list);
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        int h = 1;
        for (Value v : values) {
            h = h * 31 + v.hashCode();
        }
        hash = h;
        return h;
    }

    public Value[] getList() {
        return values;
    }

    @Override
    public int getType() {
        return Value.ARRAY;
    }

    public Class<?> getComponentType() {
        return componentType;
    }

    @Override
    public long getPrecision() {
        long p = 0;
        for (Value v : values) {
            p += v.getPrecision();
        }
        return p;
    }

    @Override
    public String getString() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getString());
        }
        return buff.append(')').toString();
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueArray v = (ValueArray) o;
        if (values == v.values) {
            return 0;
        }
        int l = values.length;
        int ol = v.values.length;
        int len = Math.min(l, ol);
        for (int i = 0; i < len; i++) {
            Value v1 = values[i];
            Value v2 = v.values[i];
            int comp = v1.compareTo(v2, mode);
            if (comp != 0) {
                return comp;
            }
        }
        return l > ol ? 1 : l == ol ? 0 : -1;
    }

    @Override
    public Object getObject() {
        int len = values.length;
        Object[] list = (Object[]) Array.newInstance(componentType, len);
        for (int i = 0; i < len; i++) {
            list[i] = values[i].getObject();
        }
        return list;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) {
        throw throwUnsupportedExceptionForType("PreparedStatement.set");
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getSQL());
        }
        if (values.length == 1) {
            buff.append(',');
        }
        return buff.append(')').toString();
    }

    @Override
    public String getTraceSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getTraceSQL());
        }
        return buff.append(')').toString();
    }

    @Override
    public int getDisplaySize() {
        long size = 0;
        for (Value v : values) {
            size += v.getDisplaySize();
        }
        return MathUtils.convertLongToInt(size);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueArray)) {
            return false;
        }
        ValueArray v = (ValueArray) other;
        if (values == v.values) {
            return true;
        }
        int len = values.length;
        if (len != v.values.length) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (!values[i].equals(v.values[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (!force) {
            return this;
        }
        int length = values.length;
        Value[] newValues = new Value[length];
        int i = 0;
        boolean modified = false;
        for (; i < length; i++) {
            Value old = values[i];
            Value v = old.convertPrecision(precision, true);
            if (v != old) {
                modified = true;
            }
            // empty byte arrays or strings have precision 0
            // they count as precision 1 here
            precision -= Math.max(1, v.getPrecision());
            if (precision < 0) {
                break;
            }
            newValues[i] = v;
        }
        if (i < length) {
            return get(componentType, Arrays.copyOf(newValues, i));
        }
        return modified ? get(componentType, newValues) : this;
    }

    public Value getValue(int i) {
        return values[i];
    }

    @Override
    public Value add(Value v) {
        ValueArray va = (ValueArray) v;
        List<Value> newValues = new ArrayList<>(values.length + va.values.length);
        newValues.addAll(Arrays.asList(values));
        newValues.addAll(Arrays.asList(va.values));
        return ValueArray.get(componentType, newValues.toArray(new Value[0]));
    }

    @Override
    public Value subtract(Value v) {
        ValueArray va = (ValueArray) v;
        List<Value> newValues = new ArrayList<>(Math.abs(values.length - va.values.length));
        newValues.addAll(Arrays.asList(values));
        newValues.removeAll(Arrays.asList(va.values));
        return ValueArray.get(componentType, newValues.toArray(new Value[0]));
    }

    @Override
    public int getMemory() {
        int memory = 24;
        for (Value v : values) {
            memory += v.getMemory() + Constants.MEMORY_POINTER;
        }
        return memory;
    }
}
