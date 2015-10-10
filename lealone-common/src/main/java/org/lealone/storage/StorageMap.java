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

import java.util.Map;
import java.util.Set;

import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;

public interface StorageMap<K, V> {

    public interface Builder {
        <K, V> StorageMap<K, V> openMap(String name);

        <K, V> StorageMap<K, V> openMap(String name, DataType valueType);

        <K, V> StorageMap<K, V> openMap(String name, DataType keyType, DataType valueType);

        String getMapName(int id);
    }

    public abstract class BuilderBase implements Builder {
        @Override
        public <K, V> StorageMap<K, V> openMap(String name) {
            return openMap(name, null);
        }

        @Override
        public <K, V> StorageMap<K, V> openMap(String name, DataType valueType) {
            if (valueType == null) {
                valueType = new ObjectDataType();
            }
            return openMap(name, new ObjectDataType(), valueType);
        }
    }

    /**
     * Get the map id. 
     *
     * @return the map id
     */
    public int getId();

    /**
     * Get the map name.
     *
     * @return the name
     */
    public String getName();

    /**
     * Get the key type.
     *
     * @return the key type
     */
    public DataType getKeyType();

    /**
     * Get the value type.
     *
     * @return the value type
     */
    public DataType getValueType();

    public boolean isClosed();

    /**
     * Get a value.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    public V get(Object key);

    /**
     * Add or replace a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    public V put(K key, V value);

    /**
     * Add a key-value pair if it does not yet exist.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return the old value if the key existed, or null otherwise
     */
    public V putIfAbsent(K key, V value);

    /**
     * Remove a key-value pair, if the key exists.
     *
     * @param key the key (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    public V remove(Object key);

    /**
     * Replace a value for an existing key.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return the old value, if the value was replaced, or null
     */
    // public V replace(K key, V value);

    /**
     * Replace a value for an existing key, if the value matches.
     *
     * @param key the key (may not be null)
     * @param oldValue the expected value
     * @param newValue the new value
     * @return true if the value was replaced
     */
    public boolean replace(K key, V oldValue, V newValue);

    public boolean containsKey(Object key);

    public boolean isEmpty();

    /**
     * Get the number of entries, as a integer. Integer.MAX_VALUE is returned if
     * there are more than this entries.
     *
     * @return the number of entries, as an integer
     */
    public int size();

    /**
     * Get the number of entries, as a long.
     *
     * @return the number of entries
     */
    public long sizeAsLong();

    /**
     * Remove all entries.
     */
    public void clear();

    /**
     * Remove map.
     */
    public void remove();

    /**
     * Get the first key, or null if the map is empty.
     *
     * @return the first key, or null
     */
    public K firstKey();

    /**
     * Get the last key, or null if the map is empty.
     *
     * @return the last key, or null
     */
    public K lastKey();

    /**
     * Get the largest key that is smaller than the given key, or null if no
     * such key exists.
     *
     * @param key the key
     * @return the result
     */
    public K lowerKey(K key);

    /**
     * Get the largest key that is smaller or equal to this key.
     *
     * @param key the key
     * @return the result
     */
    public K floorKey(K key);

    /**
     * Get the smallest key that is larger than the given key, or null if no
     * such key exists.
     *
     * @param key the key
     * @return the result
     */
    public K higherKey(K key);

    /**
     * Get the smallest key that is larger or equal to this key.
     *
     * @param key the key
     * @return the result
     */
    public K ceilingKey(K key);

    /**
     * Get the index of the given key in the map.
     * <p>
     * This is a O(log(size)) operation.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys. See also Arrays.binarySearch.
     *
     * @param key the key
     * @return the index
     */
    public long getKeyIndex(K key);

    /**
     * Get the key at the given index.
     * <p>
     * This is a O(log(size)) operation.
     *
     * @param index the index
     * @return the key
     */
    public K getKey(long index);

    /**
     * Set the volatile flag of the map.
     *
     * @param isVolatile the volatile flag
     */
    public boolean isInMemory();

    /**
     * Check whether the two values are equal.
     *
     * @param a the first value
     * @param b the second value
     * @return true if they are equal
     */
    public boolean areValuesEqual(Object a, Object b);

    /**
     * Get a cursor to iterate over a number of keys and values.
     *
     * @param from the first key to return
     * @return the cursor
     */
    public StorageMapCursor<K, V> cursor(K from);

    public Set<Map.Entry<K, V>> entrySet();

    public void save();

    public void close();
}
