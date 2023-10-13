/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.value;

import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.lealone.common.util.MathUtils;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Constants;

/**
 * Implementation of the SET data type.
 */
public class ValueSet extends Value {

    private final Class<?> componentType;
    private final Set<Value> values;
    private int hash;

    private ValueSet(Class<?> componentType, Set<?> set) {
        this.componentType = componentType;
        this.values = new HashSet<>(set.size());
        int type = getCollectionComponentTypeFromClass(componentType);
        for (Object v : set) {
            values.add(DataType.convertToValue(v, type));
        }
    }

    private ValueSet(Class<?> componentType, Value[] values) {
        this.componentType = componentType;
        this.values = new HashSet<>(values.length);
        for (Value v : values) {
            this.values.add(v);
        }
    }

    public static ValueSet get(Value[] values) {
        return new ValueSet(Object.class, values);
    }

    public static ValueSet get(Class<?> componentType, Value[] values) {
        return new ValueSet(componentType, values);
    }

    public static ValueSet get(Set<?> set) {
        return new ValueSet(Object.class, set);
    }

    public static ValueSet get(Class<?> componentType, Set<?> set) {
        return new ValueSet(componentType, set);
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

    public Set<Value> getSet() {
        return values;
    }

    @Override
    public int getType() {
        return Value.SET;
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
        ValueSet v = (ValueSet) o;
        if (values == v.values) {
            return 0;
        }
        int l = values.size();
        int ol = v.values.size();
        int len = Math.min(l, ol);
        for (int i = 0; i < len; i++) {
            // Value v1 = values.get(i);
            // Value v2 = v.values.get(i);
            // int comp = v1.compareTo(v2, mode);
            // if (comp != 0) {
            // return comp;
            // }
        }
        return l > ol ? 1 : l == ol ? 0 : -1;
    }

    @Override
    public Object getObject() {
        HashSet<Object> set = new HashSet<>(values.size());
        for (Value v : values) {
            set.add(v.getObject());
        }
        return set;
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
        if (!(other instanceof ValueSet)) {
            return false;
        }
        ValueSet v = (ValueSet) other;
        if (values == v.values) {
            return true;
        }
        int len = values.size();
        if (len != v.values.size()) {
            return false;
        }
        return values.equals(v.values);
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
        for (Value old : values) {
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
            return get(componentType, new HashSet<>(Arrays.asList(Arrays.copyOf(newValues, i))));
        }
        return modified ? get(componentType, new HashSet<>(Arrays.asList(newValues))) : this;
    }

    @Override
    public Value add(Value v) {
        ValueSet vs = (ValueSet) v;
        Set<Value> newValues = new HashSet<>(values.size() + vs.values.size());
        newValues.addAll(values);
        newValues.addAll(vs.values);
        return ValueSet.get(componentType, newValues);
    }

    @Override
    public Value subtract(Value v) {
        ValueSet vs = (ValueSet) v;
        Set<Value> newValues = new HashSet<>(Math.abs(values.size() - vs.values.size()));
        newValues.addAll(values);
        newValues.removeAll(vs.values);
        return ValueSet.get(componentType, newValues);
    }

    public void convertComponent(int type) {
        Set<Value> newValues = new HashSet<>(values.size());
        for (Value v : values) {
            newValues.add(v.convertTo(type));
        }
        values.clear();
        values.addAll(newValues);
    }
}
