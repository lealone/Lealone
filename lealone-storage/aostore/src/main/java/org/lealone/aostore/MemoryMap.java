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
package org.lealone.aostore;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.aostore.btree.BTreeMap;
import org.lealone.common.util.DataUtils;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapBuilder;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.DataType;

/**
 * A skipList-based memory map
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author zhh
 */
public class MemoryMap<K, V> extends ConcurrentSkipListMap<K, V> implements StorageMap<K, V> {
    protected final int id;
    protected final String name;
    protected final DataType keyType;
    protected final DataType valueType;

    public MemoryMap(int id, String name, DataType keyType, DataType valueType) {
        this.id = id;
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
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
    public boolean isClosed() {
        return false;
    }

    @Override
    public long sizeAsLong() {
        return size();
    }

    @Override
    public void remove() {
        clear();
    }

    @Override
    public K getKey(long index) {
        throw DataUtils.newUnsupportedOperationException("getKey");
    }

    @Override
    public long getKeyIndex(K key) {
        throw DataUtils.newUnsupportedOperationException("getKeyIndex");
    }

    @Override
    public boolean isInMemory() {
        return true;
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        return BTreeMap.areValuesEqual(valueType, a, b);
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return new Cursor<>(this, from);
    }

    @Override
    public void save() {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }

    public static class Builder<K, V> extends StorageMapBuilder<MemoryMap<K, V>, K, V> {
        @Override
        public MemoryMap<K, V> openMap() {
            return new MemoryMap<>(id, name, keyType, valueType);
        }
    }

    private static class Cursor<K, V> implements StorageMapCursor<K, V> {
        private final Iterator<Entry<K, V>> iterator;
        private Entry<K, V> entry;

        Cursor(MemoryMap<K, V> map, K from) {
            iterator = map.tailMap(from).entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public K next() {
            entry = iterator.next();
            return entry.getKey();
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        @Override
        public K getKey() {
            return entry.getKey();
        }

        @Override
        public V getValue() {
            return entry.getValue();
        }
    }

}
