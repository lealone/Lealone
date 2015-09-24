/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.lealone.transaction;

import java.util.Iterator;
import java.util.Map.Entry;

import org.lealone.storage.type.DataType;

public interface TransactionMap<K, V> {
    /**
     * Set the volatile flag of the map.
     *
     * @param isVolatile the volatile flag
     */
    public void setVolatile(boolean isVolatile);

    /**
     * Get the last key.
     *
     * @return the last key, or null if empty
     */
    public K lastKey();

    /**
     * Get the size of the raw map. This includes uncommitted entries, and
     * transiently removed entries, so it is the maximum number of entries.
     *
     * @return the maximum size
     */
    public long sizeAsLongMax();

    /**
     * Get a clone of this map for the given transaction.
     *
     * @param transaction the transaction
     * @param savepoint the savepoint
     * @return the map
     */
    public TransactionMap<K, V> getInstance(Transaction transaction, long savepoint);

    /**
     * Get the most recent value for the given key.
     *
     * @param key the key
     * @return the value or null
     */
    public V getLatest(K key);

    /**
     * Update the value for the given key.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @param value the new value (not null)
     * @return the old value
     * @throws IllegalStateException if a lock timeout occurs
     */
    public V put(K key, V value);

    /**
     * Remove an entry.
     * <p>
     * If the row is locked, this method will retry until the row could be
     * updated or until a lock timeout.
     *
     * @param key the key
     * @throws IllegalStateException if a lock timeout occurs
     */
    public V remove(K key);

    /**
     * Iterate over entries.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public Iterator<Entry<K, V>> entryIterator(final K from);

    /**
     * Get the value for the given key at the time when this map was opened.
     *
     * @param key the key
     * @return the value or null
     */
    public V get(K key);

    /**
     * Check whether this map is closed.
     *
     * @return true if closed
     */
    public boolean isClosed();

    public void removeMap();

    /**
     * Clear the map.
     */
    public void clear();

    /**
     * Get the first key.
     *
     * @return the first key, or null if empty
     */
    public K firstKey();

    /**
     * Get the size of the map as seen by this transaction.
     *
     * @return the size
     */
    public long sizeAsLong();

    public DataType getKeyType();

    /**
     * Update the value for the given key, without adding an undo log entry.
     *
     * @param key the key
     * @param value the value
     * @return the old value
     */
    public V putCommitted(K key, V value);

    /**
     * Iterate over keys.
     *
     * @param from the first key to return
     * @return the iterator
     */
    public Iterator<K> keyIterator(K from);

    /**
     * Iterate over keys.
     *
     * @param from the first key to return
     * @param includeUncommitted whether uncommitted entries should be
     *            included
     * @return the iterator
     */
    public Iterator<K> keyIterator(final K from, final boolean includeUncommitted);

    /**
     * Whether the entry for this key was added or removed from this
     * session.
     *
     * @param key the key
     * @return true if yes
     */
    public boolean isSameTransaction(K key);

    /**
     * Get one of the previous or next keys. There might be no value
     * available for the returned key.
     *
     * @param key the key (may not be null)
     * @param offset how many keys to skip (-1 for previous, 1 for next)
     * @return the key
     */
    public K relativeKey(K key, long offset);

    /**
     * Get the smallest key that is larger than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K higherKey(K key);

    /**
     * Get the largest key that is smaller than the given key, or null if no
     * such key exists.
     *
     * @param key the key (may not be null)
     * @return the result
     */
    public K lowerKey(K key);

    public int getMapId();
}
