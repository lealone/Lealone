/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;

public interface ServiceExecutor {

    public static final String NO_RETURN_VALUE = "__NO_RETURN_VALUE__";

    public default Value executeService(String methodName, Value[] methodArgs) {
        return ValueNull.INSTANCE;
    }

    public default Object executeService(String methodName, Map<String, Object> methodArgs) {
        return NO_RETURN_VALUE;
    }

    public default Object executeService(String methodName, String json) {
        return NO_RETURN_VALUE;
    }

    public default Integer toInt(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Integer.valueOf(v);
    }

    public default Long toLong(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Long.valueOf(v);
    }

    public default Byte toByte(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Byte.valueOf(v);
    }

    public default Short toShort(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Short.valueOf(v);
    }

    public default Float toFloat(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Float.valueOf(v);
    }

    public default Double toDouble(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Double.valueOf(v);
    }

    public default Boolean toBoolean(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Boolean.valueOf(v);
    }

    public default Date toDate(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Date.valueOf(v);
    }

    public default Time toTime(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Time.valueOf(v);
    }

    public default Timestamp toTimestamp(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : Timestamp.valueOf(v);
    }

    public default BigDecimal toBigDecimal(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : new BigDecimal(v);
    }

    public default Blob toBlob(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : new org.lealone.db.value.ReadonlyBlob(v);
    }

    public default Clob toClob(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : new org.lealone.db.value.ReadonlyClob(v);
    }

    public default Array toArray(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : new org.lealone.db.value.ReadonlyArray(v);
    }

    public default UUID toUUID(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : UUID.fromString(v);
    }

    public default byte[] toBytes(String key, Map<String, Object> methodArgs) {
        String v = toString(key, methodArgs);
        return v == null ? null : v.getBytes();
    }

    public default String toString(String key, Map<String, Object> methodArgs) {
        Object v = methodArgs.get(key);
        if (v == null)
            return null;
        else {
            return v.toString().trim();
        }
    }

    public default Object toObject(String key, Map<String, Object> methodArgs) {
        return methodArgs.get(key);
    }

    @SuppressWarnings("unchecked")
    public default <T> List<T> toList(String key, Map<String, Object> methodArgs) {
        Object v = methodArgs.get(key);
        if (v == null)
            return null;
        else {
            if (v instanceof List)
                return (List<T>) v;
            ArrayList<T> list = new ArrayList<>(1);
            list.add((T) v);
            return list;
        }
    }

    @SuppressWarnings("unchecked")
    public default <T> Set<T> toSet(String key, Map<String, Object> methodArgs) {
        Object v = methodArgs.get(key);
        if (v == null)
            return null;
        else {
            if (v instanceof Set)
                return (Set<T>) v;
            else if (v instanceof List)
                return new HashSet<>((List<T>) v);
            HashSet<T> set = new HashSet<>(1);
            set.add((T) v);
            return set;
        }
    }

    @SuppressWarnings("unchecked")
    public default <K, V> Map<K, V> toMap(String key, Map<String, Object> methodArgs) {
        Object v = methodArgs.get(key);
        if (v == null)
            return null;
        else {
            if (v instanceof Map)
                return (Map<K, V>) v;
            return (Map<K, V>) methodArgs;
        }
    }

    public default RuntimeException noMethodException(String methodName) {
        return new RuntimeException("no method: " + methodName);
    }
}
