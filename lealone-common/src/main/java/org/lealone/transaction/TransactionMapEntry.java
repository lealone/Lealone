/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import java.util.Map;

import org.lealone.common.util.DataUtils;
import org.lealone.storage.page.IPage;

/**
 * An entry of a transaction map.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class TransactionMapEntry<K, V> implements Map.Entry<K, V> {

    private final K key;
    private final V value;
    private final ITransactionalValue tv;
    private final IPage page;

    public TransactionMapEntry(K key, V value) {
        this(key, value, null, null);
    }

    public TransactionMapEntry(K key, V value, ITransactionalValue tv, IPage page) {
        this.key = key;
        this.value = value;
        this.tv = tv;
        this.page = page;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        throw DataUtils.newUnsupportedOperationException("Updating the value is not supported");
    }

    public ITransactionalValue getTValue() {
        return tv;
    }

    public IPage getPage() {
        return page;
    }
}
