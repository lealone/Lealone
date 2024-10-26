/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.util.DataUtils;
import com.lealone.db.value.ValueDataType.PrimaryKey;
import com.lealone.db.value.ValueLong;
import com.lealone.storage.type.ObjectDataType;
import com.lealone.storage.type.StorageDataType;

public abstract class StorageMapBase<K, V> implements StorageMap<K, V> {

    protected final String name;
    protected final StorageDataType keyType;
    protected final StorageDataType valueType;
    protected final Storage storage;

    protected final AtomicLong maxKey = new AtomicLong(0);

    protected StorageMapBase(String name, StorageDataType keyType, StorageDataType valueType,
            Storage storage) {
        DataUtils.checkNotNull(name, "name");
        if (keyType == null) {
            keyType = new ObjectDataType();
        }
        if (valueType == null) {
            valueType = new ObjectDataType();
        }
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        this.storage = storage;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public StorageDataType getKeyType() {
        return keyType;
    }

    @Override
    public StorageDataType getValueType() {
        return valueType;
    }

    @Override
    public Storage getStorage() {
        return storage;
    }

    // 如果新key比maxKey大就更新maxKey
    // 允许多线程并发更新
    @Override
    public void setMaxKey(K key) {
        if (key instanceof PrimaryKey) {
            setMaxKey(((PrimaryKey) key).getKey());
        } else if (key instanceof ValueLong) {
            setMaxKey(((ValueLong) key).getLong());
        } else if (key instanceof Number) {
            setMaxKey(((Number) key).longValue());
        }
    }

    private void setMaxKey(long key) {
        while (true) {
            long old = maxKey.get();
            if (key > old) {
                if (maxKey.compareAndSet(old, key))
                    break;
            } else {
                break;
            }
        }
    }

    @Override
    public long getAndAddKey(long delta) {
        return maxKey.getAndAdd(delta);
    }

    public long getMaxKey() {
        return maxKey.get();
    }

    public long incrementAndGetMaxKey() {
        return maxKey.incrementAndGet();
    }

    private final ConcurrentHashMap<Object, Object> oldValueCache = new ConcurrentHashMap<>(1);

    @Override
    public ConcurrentHashMap<Object, Object> getOldValueCache() {
        return oldValueCache;
    }
}
