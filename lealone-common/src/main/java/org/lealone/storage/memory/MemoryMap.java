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
package org.lealone.storage.memory;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;

/**
 * A skipList-based memory map
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author zhh
 */
public class MemoryMap<K, V> implements StorageMap<K, V> {

    private static class KeyComparator<K> implements java.util.Comparator<K> {
        DataType keyType;

        public KeyComparator(DataType keyType) {
            this.keyType = keyType;
        }

        @Override
        public int compare(K k1, K k2) {
            return keyType.compare(k1, k2);
        }
    }

    protected final int id;
    protected final String name;
    protected final DataType keyType;
    protected final DataType valueType;
    protected final ConcurrentSkipListMap<K, V> skipListMap;

    protected boolean closed;

    public MemoryMap(int id, String name, DataType keyType, DataType valueType) {
        if (keyType == null)
            keyType = new ObjectDataType();
        if (valueType == null)
            valueType = new ObjectDataType();

        this.id = id;
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        skipListMap = new ConcurrentSkipListMap<>(new KeyComparator<K>(keyType));
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public DataType getKeyType() {
        return keyType;
    }

    @Override
    public DataType getValueType() {
        return valueType;
    }

    @Override
    public V get(K key) {
        return skipListMap.get(key);
    }

    @Override
    public V put(K key, V value) {
        return skipListMap.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return skipListMap.putIfAbsent(key, value);
    }

    @Override
    public V remove(K key) {
        return skipListMap.remove(key);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return skipListMap.replace(key, oldValue, newValue);
    }

    @Override
    public K firstKey() {
        try {
            return skipListMap.firstKey();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public K lastKey() {
        try {
            return skipListMap.lastKey();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public K lowerKey(K key) { // 小于给定key的最大key
        try {
            return skipListMap.lowerKey(key);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public K floorKey(K key) { // 小于或等于给定key的最大key
        try {
            return skipListMap.floorKey(key);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public K higherKey(K key) { // 大于给定key的最小key
        try {
            return skipListMap.higherKey(key);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public K ceilingKey(K key) { // 大于或等于给定key的最小key
        try {
            return skipListMap.ceilingKey(key);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return valueType.compare(a, b) == 0;
    }

    @Override
    public int size() {
        return skipListMap.size();
    }

    @Override
    public long sizeAsLong() {
        return size();
    }

    @Override
    public boolean containsKey(K key) {
        return skipListMap.containsKey(key);
    }

    @Override
    public boolean isEmpty() {
        return skipListMap.isEmpty();
    }

    @Override
    public boolean isInMemory() {
        return true;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return new MemoryMapCursor<>(from == null ? skipListMap.entrySet().iterator() : skipListMap.tailMap(from)
                .entrySet().iterator());
    }

    @Override
    public void clear() {
        skipListMap.clear();
    }

    @Override
    public void remove() {
        clear();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        clear();
        closed = true;
    }

    @Override
    public void save() {
    }
}
