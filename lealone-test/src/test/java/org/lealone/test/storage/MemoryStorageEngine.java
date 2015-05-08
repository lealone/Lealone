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
package org.lealone.test.storage;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.engine.MVStorageEngine;
import org.lealone.engine.StorageEngineManager;
import org.lealone.engine.StorageMap;
import org.lealone.test.misc.CRUDExample;
import org.lealone.type.DataType;
import org.lealone.type.ObjectDataType;

public class MemoryStorageEngine extends MVStorageEngine {
    public static void main(String[] args) throws Exception {
        MemoryStorageEngine mse = new MemoryStorageEngine();
        StorageEngineManager.registerStorageEngine(mse);

        CRUDExample.setStorageEngineName(mse.getName());
        CRUDExample.main(args);
    }

    public MemoryStorageEngine() {
        super(new MemoryMapBuilder());
    }

    @Override
    public String getName() {
        return "memory";
    }

    static class MemoryMapBuilder extends StorageMap.BuilderBase {
        @Override
        public <K, V> StorageMap<K, V> openMap(String name, DataType keyType, DataType valueType) {
            return new MemoryMap<K, V>(name, keyType, valueType);
        }
    }

    static class MemoryCursor<K, V> implements StorageMap.Cursor<K, V> {
        private final Iterator<Entry<K, V>> iterator;
        private Entry<K, V> e;

        public MemoryCursor(Iterator<Entry<K, V>> iterator, K from, DataType keyType) {
            this.iterator = iterator;
            if (from != null)
                while (hasNext()) {
                    K k = next();
                    if (MemoryMap.areEqual(k, from, keyType)) {
                        break;
                    }
                }
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public K next() {
            e = iterator.next();
            return e.getKey();
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        @Override
        public K getKey() {
            return e.getKey();
        }

        @Override
        public V getValue() {
            return e.getValue();
        }

    }

    static class MemoryMap<K, V> extends ConcurrentSkipListMap<K, V> implements StorageMap<K, V> {

        private static final AtomicInteger counter = new AtomicInteger(0);

        private final String name;
        private final DataType keyType;
        private final DataType valueType;
        private final int id;

        public MemoryMap(String name) {
            this(name, new ObjectDataType(), new ObjectDataType());
        }

        public MemoryMap(String name, DataType keyType, DataType valueType) {
            this.name = name;
            this.keyType = keyType;
            this.valueType = valueType;
            id = counter.incrementAndGet();
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
        public V put(K key, V value) {
            return super.put(key, value);
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
        public long getKeyIndex(K key) {
            long index = -1;
            for (K k : keySet()) {
                index++;
                if (areEqual(key, k, keyType))
                    break;
            }

            return index;
        }

        @Override
        public K getKey(long index) {
            if (index < 0)
                return null;

            long i = 0;
            K key = null;
            for (K k : keySet()) {
                if (i == index) {
                    key = k;
                    break;
                }

                i++;
            }
            if (index != i)
                return null;

            return key;
        }

        @Override
        public void setVolatile(boolean isVolatile) {
        }

        @Override
        public boolean areValuesEqual(Object a, Object b) {
            return areEqual(a, b, valueType);
        }

        private static boolean areEqual(Object a, Object b, DataType dataType) {
            if (a == b) {
                return true;
            } else if (a == null || b == null) {
                return false;
            }
            return dataType.compare(a, b) == 0;
        }

        @Override
        public StorageMap.Cursor<K, V> cursor(K from) {
            return new MemoryCursor<>(entrySet().iterator(), from, keyType);
        }

        @Override
        public K firstKey() {
            try {
                return super.firstKey();
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        @Override
        public K lastKey() {
            try {
                return super.lastKey();
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        @Override
        public K lowerKey(K key) { //小于给定key的最大key
            try {
                return super.lowerKey(key);
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        @Override
        public K floorKey(K key) { //小于或等于给定key的最大key
            try {
                return super.floorKey(key);
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        @Override
        public K higherKey(K key) { //大于给定key的最小key
            try {
                return super.higherKey(key);
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        @Override
        public K ceilingKey(K key) { //大于或等于给定key的最小key
            try {
                return super.ceilingKey(key);
            } catch (NoSuchElementException e) {
                return null;
            }
        }
    }
}
