/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import java.util.Map;

import org.lealone.common.util.DataUtils;

/**
 * An entry of a transaction map.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class TransactionMapEntry<K, V> implements Map.Entry<K, V> {

    private final K key;
    private final V value;
    private final Object rawValue;

    public TransactionMapEntry(K key, V value) {
        this(key, value, null);
    }

    public TransactionMapEntry(K key, V value, Object rawValue) {
        this.key = key;
        this.value = value;
        this.rawValue = rawValue;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    public Object getRawValue() {
        return rawValue;
    }

    @Override
    public V setValue(V value) {
        throw DataUtils.newUnsupportedOperationException("Updating the value is not supported");
    }
}