/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.value;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.lealone.common.util.MathUtils;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Constants;

/**
 * Implementation of the MAP data type.
 */
public class ValueMap extends Value {

    private final Class<?> kType;
    private final Class<?> vType;
    private final Map<Value, Value> map;
    private int hash;

    private ValueMap(Class<?> kType, Class<?> vType, Map<?, ?> map) {
        this.kType = kType;
        this.vType = vType;
        this.map = new HashMap<>(map.size());
        int kt = getCollectionComponentTypeFromClass(kType);
        int vt = getCollectionComponentTypeFromClass(vType);
        for (Entry<?, ?> e : map.entrySet()) {
            this.map.put(DataType.convertToValue(e.getKey(), kt),
                    DataType.convertToValue(e.getValue(), vt));
        }
    }

    private ValueMap(Class<?> kType, Class<?> vType, Value[] values) {
        this.kType = kType;
        this.vType = vType;
        this.map = new HashMap<>(values.length / 2);
        for (int i = 0; i < values.length; i += 2) {
            this.map.put(values[i], values[i + 1]);
        }
    }

    public static ValueMap get(Value[] values) {
        return new ValueMap(Object.class, Object.class, values);
    }

    public static ValueMap get(Class<?> kType, Class<?> vType, Value[] values) {
        return new ValueMap(kType, vType, values);
    }

    public static ValueMap get(Map<?, ?> map) {
        return new ValueMap(Object.class, Object.class, map);
    }

    public static ValueMap get(Class<?> kType, Class<?> vType, Map<?, ?> map) {
        return new ValueMap(kType, vType, map);
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        hash = map.hashCode();
        return hash;
    }

    public Map<Value, Value> getMap() {
        return map;
    }

    @Override
    public int getType() {
        return Value.MAP;
    }

    public Class<?> getKeyType() {
        return kType;
    }

    public Class<?> getValueType() {
        return vType;
    }

    @Override
    public long getPrecision() {
        long p = 0;
        for (Entry<Value, Value> e : map.entrySet()) {
            p += e.getKey().getPrecision() + e.getValue().getPrecision();
        }
        return p;
    }

    @Override
    public String getString() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Entry<Value, Value> e : map.entrySet()) {
            buff.appendExceptFirst(", ");
            buff.append(e.getKey().getString());
            buff.append(":");
            buff.append(e.getValue().getString());
        }
        return buff.append(')').toString();
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueMap v = (ValueMap) o;
        if (map == v.map) {
            return 0;
        }
        int l = map.size();
        int ol = v.map.size();
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
        HashMap<Object, Object> map = new HashMap<>(this.map.size());
        for (Entry<Value, Value> e : this.map.entrySet()) {
            map.put(e.getKey().getObject(), e.getValue().getObject());
        }
        return map;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) {
        throw throwUnsupportedExceptionForType("PreparedStatement.set");
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Entry<Value, Value> e : map.entrySet()) {
            buff.appendExceptFirst(", ");
            buff.append(e.getKey().getSQL());
            buff.append(":");
            buff.append(e.getValue().getSQL());
        }
        if (map.size() == 1) {
            buff.append(',');
        }
        return buff.append(')').toString();
    }

    @Override
    public String getTraceSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Entry<Value, Value> e : map.entrySet()) {
            buff.appendExceptFirst(", ");
            buff.append(e.getKey().getTraceSQL());
            buff.append(":");
            buff.append(e.getValue().getTraceSQL());
        }
        return buff.append(')').toString();
    }

    @Override
    public int getDisplaySize() {
        long size = 0;
        for (Entry<Value, Value> e : map.entrySet()) {
            size += e.getKey().getDisplaySize() + e.getValue().getDisplaySize();
        }
        return MathUtils.convertLongToInt(size);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueMap)) {
            return false;
        }
        ValueMap v = (ValueMap) other;
        if (map == v.map) {
            return true;
        }
        return map.equals(v.map);
    }

    @Override
    public int getMemory() {
        int memory = 32;
        for (Entry<Value, Value> e : map.entrySet()) {
            memory += e.getKey().getMemory() + e.getValue().getMemory() + Constants.MEMORY_POINTER;
        }
        return memory;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (!force) {
            return this;
        }
        HashMap<Value, Value> newMap = new HashMap<>(map.size());
        for (Entry<Value, Value> e : map.entrySet()) {
            newMap.put(e.getKey().convertPrecision(precision, true),
                    e.getValue().convertPrecision(precision, true));
        }
        return get(kType, vType, newMap);
    }

    @Override
    public Value add(Value v) {
        ValueMap vm = (ValueMap) v;
        Map<Value, Value> newMap = new HashMap<>(map.size() + vm.map.size());
        newMap.putAll(map);
        newMap.putAll(vm.map);
        return ValueMap.get(kType, vType, newMap);
    }

    @Override
    public Value subtract(Value v) {
        HashSet<Value> set = new HashSet<>();
        if (v instanceof ValueSet) {
            ValueSet vs = (ValueSet) v;
            set.addAll(vs.getSet());
        } else {
            ValueMap vm = (ValueMap) v;
            set.addAll(vm.map.keySet());
        }
        Map<Value, Value> newMap = new HashMap<>(Math.abs(map.size() - set.size()));
        newMap.putAll(map);
        for (Value key : set) {
            newMap.remove(key);
        }
        return ValueMap.get(kType, vType, newMap);
    }

    public void convertComponent(int kType, int vType) {
        Map<Value, Value> newMap = new HashMap<>(map.size());
        for (Entry<Value, Value> e : map.entrySet()) {
            newMap.put(e.getKey().convertTo(kType), e.getValue().convertTo(vType));
        }
        map.clear();
        map.putAll(newMap);
    }
}
