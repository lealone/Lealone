/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.async.Future;
import org.lealone.db.session.Session;
import org.lealone.db.value.ValueLong;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;

public abstract class StorageMapBase<K, V> implements StorageMap<K, V> {

    protected final String name;
    protected final StorageDataType keyType;
    protected final StorageDataType valueType;
    protected final Storage storage;

    protected final AtomicLong maxKey = new AtomicLong(0);

    protected StorageMapBase(String name, StorageDataType keyType, StorageDataType valueType, Storage storage) {
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

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public long getMemorySpaceUsed() {
        return 0;
    }

    ////////////////////// 以下是分布式API， 默认直接抛出异常 ////////////////////////////////

    @Override
    public Future<Object> get(Session session, Object key) {
        throw DbException.getUnsupportedException("get");
    }

    @Override
    public Future<Object> put(Session session, Object key, Object value, StorageDataType valueType,
            boolean addIfAbsent) {
        throw DbException.getUnsupportedException("put");
    }

    @Override
    public Future<Object> append(Session session, Object value, StorageDataType valueType) {
        throw DbException.getUnsupportedException("append");
    }

    @Override
    public Future<Boolean> replace(Session session, Object key, Object oldValue, Object newValue,
            StorageDataType valueType) {
        throw DbException.getUnsupportedException("replace");
    }

    @Override
    public Future<Object> remove(Session session, Object key) {
        throw DbException.getUnsupportedException("remove");
    }

    @Override
    public void addLeafPage(PageKey pageKey, ByteBuffer page, boolean addPage) {
        throw DbException.getUnsupportedException("addLeafPage");
    }

    @Override
    public void removeLeafPage(PageKey pageKey) {
        throw DbException.getUnsupportedException("removeLeafPage");
    }

    @Override
    public LeafPageMovePlan prepareMoveLeafPage(LeafPageMovePlan leafPageMovePlan) {
        throw DbException.getUnsupportedException("prepareMoveLeafPage");
    }

    @Override
    public ByteBuffer readPage(PageKey pageKey) {
        throw DbException.getUnsupportedException("readPage");
    }

    @Override
    public Map<String, List<PageKey>> getNodeToPageKeyMap(Session session, K from, K to) {
        throw DbException.getUnsupportedException("getNodeToPageKeyMap");
    }
}
