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
package org.lealone.storage;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.common.util.DataUtils;
import org.lealone.storage.type.DataType;

/**
 * A write optimization buffered map.
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author zhh
 */
@SuppressWarnings("unchecked")
public class BufferedMap<K, V> implements StorageMap<K, V>, Callable<Void> {
    private final StorageMap<K, V> map;
    private final ConcurrentSkipListMap<Object, Object> buffer = new ConcurrentSkipListMap<>();

    public BufferedMap(StorageMap<K, V> map) {
        this.map = map;
    }

    public StorageMap<K, V> getMap() {
        return map;
    }

    @Override
    public int getId() {
        return map.getId();
    }

    @Override
    public String getName() {
        return map.getName();
    }

    @Override
    public DataType getKeyType() {
        return map.getKeyType();
    }

    @Override
    public DataType getValueType() {
        return map.getValueType();
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
        return (V) buffer.put(key, value);
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
        Object v = buffer.remove(key);
        if (v == null)
            v = map.remove(key);

        return (V) v;
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

    private static class Cursor<K, V> implements StorageMapCursor<K, V> {
        private final Iterator<Entry<Object, Object>> iterator;
        private final StorageMapCursor<K, V> cursor;
        private final DataType keyType;

        private Entry<Object, Object> iteratorEntry;
        private K cursorKey;
        private K key;
        private V value;

        Cursor(BufferedMap<K, V> bmap, K from) {
            iterator = bmap.buffer.tailMap(from).entrySet().iterator();
            cursor = bmap.map.cursor(from);
            keyType = bmap.map.getKeyType();
        }

        @Override
        public boolean hasNext() {
            if (iterator.hasNext())
                return true;
            return cursor.hasNext();
        }

        @Override
        public K next() {
            if (iteratorEntry == null)
                iteratorEntry = iterator.next();
            if (cursorKey == null)
                cursorKey = cursor.next();

            if (keyType.compare(iteratorEntry.getKey(), cursorKey) < 0) {
                key = (K) iteratorEntry.getKey();
                value = (V) iteratorEntry.getValue();
                iteratorEntry = null;
            } else {
                key = cursorKey;
                value = cursor.getValue();
                cursorKey = null;
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
