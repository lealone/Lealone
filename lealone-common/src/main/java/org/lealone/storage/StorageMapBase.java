/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.util.DataUtils;
import org.lealone.db.value.ValueLong;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;

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

    @Override
    @SuppressWarnings("unchecked")
    public K append(V value) {
        K key = (K) ValueLong.get(maxKey.incrementAndGet());
        put(key, value);
        return key;
    }

    // 如果新key比maxKey大就更新maxKey
    // 允许多线程并发更新
    @Override
    public void setMaxKey(K key) {
        if (key instanceof ValueLong) {
            long k = ((ValueLong) key).getLong();
            while (true) {
                long old = maxKey.get();
                if (k > old) {
                    if (maxKey.compareAndSet(old, k))
                        break;
                } else {
                    break;
                }
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

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public long getMemorySpaceUsed() {
        return 0;
    }
}
