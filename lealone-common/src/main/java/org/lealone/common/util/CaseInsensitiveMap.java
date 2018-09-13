/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.common.util;

import java.util.HashMap;
import java.util.Map;

/**
 * A hash map with a case-insensitive string key.
 *
 * @param <V> the value type
 */
public class CaseInsensitiveMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = 1L;

    public CaseInsensitiveMap() {
    }

    public CaseInsensitiveMap(int initialCapacity) {
        super(initialCapacity);
    }

    public CaseInsensitiveMap(Map<? extends String, ? extends V> m) {
        this(m.size());
        putAll(m);
    }

    @Override
    public V get(Object key) {
        return super.get(toUpper(key));
    }

    @Override
    public V put(String key, V value) {
        return super.put(toUpper(key), value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        for (Map.Entry<? extends String, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(toUpper(key));
    }

    @Override
    public V remove(Object key) {
        return super.remove(toUpper(key));
    }

    private static String toUpper(Object key) {
        return key == null ? null : StringUtils.toUpperEnglish(key.toString());
    }

}
