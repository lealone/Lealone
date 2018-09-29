/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.storage.aose;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.util.DataUtils;
import org.lealone.db.value.ValueLong;
import org.lealone.storage.DelegatedStorageMap;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.StorageDataType;

/**
 * A write optimization buffered map.
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author zhh
 */
@SuppressWarnings("unchecked")
public class BufferedMap<K, V> extends DelegatedStorageMap<K, V> implements Callable<Void> {

    private final ConcurrentSkipListMap<Object, Object> buffer = new ConcurrentSkipListMap<>();
    private final AtomicLong lastKey = new AtomicLong(0);

    public BufferedMap(StorageMap<K, V> map) {
        super(map);
        if (map instanceof StorageMapBase) {
            lastKey.set(((StorageMapBase<K, V>) map).getLastKey());
        }
    }

    public StorageMap<K, V> getMap() {
        return map;
    }

    @Override
    public V get(K key) {
        Object v = buffer.get(key);
        if (v == null)
            v = map.get(key);
        return (V) v;
    }

    @Override
    public V put(K key, V value) {
        if (map instanceof StorageMapBase) {
            ((StorageMapBase<K, V>) map).setLastKey(key);
            setLastKey(key);
        }
        return (V) buffer.put(key, value);
    }

    private void setLastKey(Object key) {
        if (key instanceof ValueLong) {
            long k = ((ValueLong) key).getLong();
            while (true) {
                long old = lastKey.get();
                if (k > old) {
                    if (lastKey.compareAndSet(old, k))
                        break;
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V old = get(key);
        if (old == null) {
            put(key, value);
        }
        return old;
    }

    @Override
    public V remove(K key) {
        Object v1 = buffer.remove(key);
        Object v2 = map.remove(key);

        if (v1 == null)
            v1 = v2;

        return (V) v1;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        V old = get(key);
        if (areValuesEqual(old, oldValue)) {
            put(key, newValue);
            return true;
        }
        return false;
    }

    @Override
    public K firstKey() {
        return getFirstLast(true);
    }

    @Override
    public K lastKey() {
        return getFirstLast(false);
    }

    protected K getFirstLast(boolean first) {
        Object k, k1, k2;
        try {
            if (first)
                k1 = buffer.firstKey();
            else
                k1 = buffer.lastKey();
        } catch (NoSuchElementException e) {
            k1 = null;
        }
        if (first)
            k2 = map.firstKey();
        else
            k2 = map.lastKey();

        if (k1 == null)
            k = k2;
        else if (k2 == null)
            k = k1;
        else {
            if (first)
                k = getKeyType().compare(k1, k2) < 0 ? k1 : k2;
            else
                k = getKeyType().compare(k1, k2) < 0 ? k2 : k1;
        }

        return (K) k;
    }

    @Override
    public K lowerKey(K key) {
        return getMinMax(key, true, true);
    }

    @Override
    public K floorKey(K key) {
        return getMinMax(key, true, false);
    }

    @Override
    public K higherKey(K key) {
        return getMinMax(key, false, true);
    }

    @Override
    public K ceilingKey(K key) {
        return getMinMax(key, false, false);
    }

    protected K getMinMax(K key, boolean min, boolean excluding) {
        Object k, k1, k2;
        try {
            if (key == null) {
                k1 = buffer.firstKey();
            } else {
                if (!min) {
                    if (excluding)
                        k1 = buffer.higherKey(key);
                    else
                        k1 = buffer.ceilingKey(key);
                } else {
                    if (excluding)
                        k1 = buffer.lowerKey(key);
                    else
                        k1 = buffer.floorKey(key);
                }
            }
        } catch (NoSuchElementException e) {
            k1 = null;
        }
        if (!min) {
            if (excluding)
                k2 = map.higherKey(key);
            else
                k2 = map.ceilingKey(key);
        } else {
            if (excluding)
                k2 = map.lowerKey(key);
            else
                k2 = map.floorKey(key);
        }

        if (k1 == null)
            k = k2;
        else if (k2 == null)
            k = k1;
        else {
            if (!min) {
                k = getKeyType().compare(k1, k2) < 0 ? k1 : k2;
            } else {
                k = getKeyType().compare(k1, k2) < 0 ? k2 : k1;
            }
        }
        return (K) k;
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        return map.areValuesEqual(a, b);
    }

    @Override
    public int size() {
        return buffer.size() + map.size();
    }

    @Override
    public long sizeAsLong() {
        return buffer.size() + map.sizeAsLong();
    }

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        return buffer.isEmpty() && map.isEmpty();
    }

    @Override
    public boolean isInMemory() {
        return map.isInMemory();
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return new Cursor<>(this, from);
    }

    @Override
    public StorageMapCursor<K, V> cursor(List<PageKey> pageKeys, K from) {
        return new Cursor<>(pageKeys, this, from);
    }

    @Override
    public void clear() {
        buffer.clear();
        map.clear();
    }

    @Override
    public void remove() {
        buffer.clear();
        map.remove();
    }

    @Override
    public boolean isClosed() {
        return map.isClosed();
    }

    @Override
    public void close() {
        buffer.clear();
        map.close();
        AOStorageService.removeBufferedMap(this);
    }

    @Override
    public void save() {
        map.save();
    }

    @Override
    public Void call() throws Exception {
        merge();
        return null;
    }

    public void merge() {
        for (Object key : buffer.keySet()) {
            // 不能先remove再put，因为刚刚remove后，要是在put之前有一个读线程进来，那么它就读不到值了
            Object value = buffer.get(key);
            map.put((K) key, (V) value);
            buffer.remove(key);
        }
    }

    public boolean needMerge() {
        return !buffer.isEmpty();
    }

    @Override
    public K append(V value) {
        K key = (K) ValueLong.get(lastKey.incrementAndGet());
        put(key, value);
        return key;
    }

    // 需要轮流从bufferIterator和mapCursor中取出一个值，哪个小先返回它
    // 保证所有返回的值整体上是排序好的
    private static class Cursor<K, V> implements StorageMapCursor<K, V> {
        private final Iterator<Entry<Object, Object>> bufferIterator;
        private final StorageMapCursor<K, V> mapCursor;
        private final StorageDataType keyType;

        private boolean bufferIteratorEnd;
        private boolean mapCursorEnd;

        private Entry<Object, Object> bufferIteratorEntry;
        private K mapCursorKey;

        private K key;
        private V value;

        Cursor(BufferedMap<K, V> bmap, K from) {
            this(null, bmap, from);
        }

        Cursor(List<PageKey> pageKeys, BufferedMap<K, V> bmap, K from) {
            if (from == null)
                bufferIterator = bmap.buffer.entrySet().iterator();
            else
                bufferIterator = bmap.buffer.tailMap(from).entrySet().iterator();
            mapCursor = bmap.map.cursor(pageKeys, from);
            keyType = bmap.map.getKeyType();
        }

        @Override
        public boolean hasNext() {
            if (bufferIteratorEnd)
                return mapCursor.hasNext();

            if (bufferIterator.hasNext())
                return true;
            else
                bufferIteratorEnd = true;
            return mapCursor.hasNext();
        }

        @Override
        public K next() {
            if (bufferIteratorEnd) {
                if (mapCursorKey != null) { // bufferIterator结束时可能上次的mapCursorKey还没有返回，所以直接利用
                    key = mapCursorKey;
                    value = mapCursor.getValue();
                    mapCursorKey = null;
                } else {
                    key = mapCursor.next();
                    value = mapCursor.getValue();
                }
            } else if (mapCursorEnd) {
                Entry<Object, Object> e;
                if (bufferIteratorEntry != null) { // mapCursor结束时可能上次的bufferIteratorEntry还没有返回，所以直接利用
                    e = bufferIteratorEntry;
                    bufferIteratorEntry = null;
                } else {
                    e = bufferIterator.next();
                }
                key = (K) e.getKey();
                value = (V) e.getValue();
            } else {
                if (bufferIteratorEntry == null)
                    bufferIteratorEntry = bufferIterator.next(); // 不需要判断hasNext()，因为会事先调用Cursor类的hasNext()

                if (mapCursorKey == null) {
                    if (mapCursor.hasNext())
                        mapCursorKey = mapCursor.next();
                    else {
                        mapCursorEnd = true;
                        return next();
                    }
                }

                int result = keyType.compare(bufferIteratorEntry.getKey(), mapCursorKey);
                if (result <= 0) {
                    key = (K) bufferIteratorEntry.getKey();
                    value = (V) bufferIteratorEntry.getValue();
                    bufferIteratorEntry = null; // 下次bufferIterator要执行next

                    // 相等时，使用bufferIterator中的，mapCursor下次也要next，
                    // 有些上层Map(比如MVCCTransactionMap)会把remove操作变成put操作，
                    // 把null值封装在一个VersionedValue中，然后调用get时取出VersionedValue，
                    // 里面是null值的话，等事务提交后再从最原始的Map中删除它
                    if (result == 0)
                        mapCursorKey = null;
                } else {
                    key = mapCursorKey;
                    value = mapCursor.getValue();
                    mapCursorKey = null; // 下次mapCursor要执行next
                }
            }
            return key;
        }

        @Override
        public void remove() {
            throw DataUtils.newUnsupportedOperationException("remove");
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }
    }
}
