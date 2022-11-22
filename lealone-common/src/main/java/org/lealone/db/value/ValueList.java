/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.value;

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.lealone.common.util.MathUtils;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Constants;

/**
 * Implementation of the LIST data type.
 */
public class ValueList extends Value {

    private final Class<?> componentType;
    private final List<Value> values;
    private int hash;

    private ValueList(Class<?> componentType, List<?> list) {
        this.componentType = componentType;
        this.values = new ArrayList<>(list.size());
        int type = getCollectionComponentTypeFromClass(componentType);
        for (Object v : list) {
            values.add(DataType.convertToValue(v, type));
        }
    }

    private ValueList(Class<?> componentType, Value[] values) {
        this.componentType = componentType;
        this.values = new ArrayList<>(values.length);
        for (Value v : values) {
            this.values.add(v);
        }
    }

    public static ValueList get(Value[] values) {
        return new ValueList(Object.class, values);
    }

    public static ValueList get(Class<?> componentType, Value[] values) {
        return new ValueList(componentType, values);
    }

    public static ValueList get(List<?> list) {
        return new ValueList(Object.class, list);
    }

    public static ValueList get(Class<?> componentType, List<?> list) {
        return new ValueList(componentType, list);
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

    public List<Value> getList() {
        return values;
    }

    @Override
    public int getType() {
        return Value.LIST;
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
        ValueList v = (ValueList) o;
        if (values == v.values) {
            return 0;
        }
        int l = values.size();
        int ol = v.values.size();
        int len = Math.min(l, ol);
        for (int i = 0; i < len; i++) {
            Value v1 = values.get(i);
            Value v2 = v.values.get(i);
            int comp = v1.compareTo(v2, mode);
            if (comp != 0) {
                return comp;
            }
        }
        return l > ol ? 1 : l == ol ? 0 : -1;
    }

    @Override
    public Object getObject() {
        int len = values.size();
        Object[] list = (Object[]) Array.newInstance(componentType, len);
        for (int i = 0; i < len; i++) {
            list[i] = values.get(i).getObject();
        }
        return Arrays.asList(list);
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
        if (values.size() == 1) {
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
        if (!(other instanceof ValueList)) {
            return false;
        }
        ValueList v = (ValueList) other;
        if (values == v.values) {
            return true;
        }
        int len = values.size();
        if (len != v.values.size()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (!values.get(i).equals(v.values.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getMemory() {
        int memory = 32;
        for (Value v : values) {
            memory += v.getMemory() + Constants.MEMORY_POINTER;
        }
        return memory;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (!force) {
            return this;
        }
        int length = values.size();
        Value[] newValues = new Value[length];
        int i = 0;
        boolean modified = false;
        for (; i < length; i++) {
            Value old = values.get(i);
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
            return get(componentType, Arrays.asList(Arrays.copyOf(newValues, i)));
        }
        return modified ? get(componentType, Arrays.asList(newValues)) : this;
    }

    public Value getValue(int i) {
        return values.get(i);
    }

    @Override
    public Value add(Value v) {
        ValueList vl = (ValueList) v;
        List<Value> newValues = new ArrayList<>(values.size() + vl.values.size());
        newValues.addAll(values);
        newValues.addAll(vl.values);
        return ValueList.get(componentType, newValues);
    }

    @Override
    public Value subtract(Value v) {
        ValueList vl = (ValueList) v;
        List<Value> newValues = new ArrayList<>(Math.abs(values.size() - vl.values.size()));
        newValues.addAll(values);
        newValues.removeAll(vl.values);
        return ValueList.get(componentType, newValues);
    }
}
