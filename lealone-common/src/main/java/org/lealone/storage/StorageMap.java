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

import org.lealone.storage.type.DataType;

public interface StorageMap<K, V> {

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

    /**
     * Get a value.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    public V get(K key);

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
    public V remove(K key);

    /**
     * Replace a value for an existing key, if the value matches.
     *
     * @param key the key (may not be null)
     * @param oldValue the expected value
     * @param newValue the new value
     * @return true if the value was replaced
     */
    public boolean replace(K key, V oldValue, V newValue);

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
     * Check whether the two values are equal.
     *
     * @param a the first value
     * @param b the second value
     * @return true if they are equal
     */
    public boolean areValuesEqual(Object a, Object b);

    /**
     * Get the number of entries.
     *
     * @return the number of entries
     */
    public int size();

    /**
     * Get the number of entries, as a long.
     *
     * @return the number of entries
     */
    public long sizeAsLong();

    public boolean containsKey(K key);

    public boolean isEmpty();

    public boolean isInMemory();

    /**
     * Get a cursor to iterate over a number of keys and values.
     *
     * @param from the first key to return
     * @return the cursor
     */
    public StorageMapCursor<K, V> cursor(K from);

    /**
     * Remove all entries.
     */
    public void clear();

    /**
     * Remove map.
     */
    public void remove();

    public boolean isClosed();

    public void close();

    public void save();
}
