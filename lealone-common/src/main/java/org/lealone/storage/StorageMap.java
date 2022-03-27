/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.db.session.Session;
import org.lealone.storage.page.LeafPageMovePlan;
import org.lealone.storage.page.PageKey;
import org.lealone.storage.type.StorageDataType;

public interface StorageMap<K, V> {

    /**
     * Get the map name.
     *
     * @return the name
     */
    String getName();

    /**
     * Get the key type.
     *
     * @return the key type
     */
    StorageDataType getKeyType();

    /**
     * Get the value type.
     *
     * @return the value type
     */
    StorageDataType getValueType();

    /**
     * Get the storage.
     *
     * @return the storage
     */
    Storage getStorage();

    /**
     * Get a value.
     *
     * @param key the key
     * @return the value, or null if not found
     */
    V get(K key);

    default V get(K key, int[] columnIndexes) {
        return get(key);
    }

    /**
     * Add or replace a key-value pair.
     *
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    V put(K key, V value);

    /**
     * Add a key-value pair if it does not yet exist.
     *
     * @param key the key (may not be null)
     * @param value the new value
     * @return the old value if the key existed, or null otherwise
     */
    V putIfAbsent(K key, V value);

    /**
     * Remove a key-value pair, if the key exists.
     *
     * @param key the key (may not be null)
     * @return the old value if the key existed, or null otherwise
     */
    V remove(K key);

    /**
     * Replace a value for an existing key, if the value matches.
     *
     * @param key the key (may not be null)
     * @param oldValue the expected value
     * @param newValue the new value
     * @return true if the value was replaced
     */
    boolean replace(K key, V oldValue, V newValue);

    K append(V value);

    void setMaxKey(K key);

    long getAndAddKey(long delta);

    /**
     * Get the first key, or null if the map is empty.
     *
     * @return the first key, or null
     */
    K firstKey();

    /**
     * Get the last key, or null if the map is empty.
     *
     * @return the last key, or null
     */
    K lastKey();

    /**
     * Get the largest key that is smaller than the given key, or null if no such key exists.
     *
     * @param key the key
     * @return the result
     */
    K lowerKey(K key);

    /**
     * Get the largest key that is smaller or equal to this key.
     *
     * @param key the key
     * @return the result
     */
    K floorKey(K key);

    /**
     * Get the smallest key that is larger than the given key, or null if no such key exists.
     *
     * @param key the key
     * @return the result
     */
    K higherKey(K key);

    /**
     * Get the smallest key that is larger or equal to this key.
     *
     * @param key the key
     * @return the result
     */
    K ceilingKey(K key);

    /**
     * Check whether the two values are equal.
     *
     * @param a the first value
     * @param b the second value
     * @return true if they are equal
     */
    boolean areValuesEqual(Object a, Object b);

    /**
     * Get the number of entries.
     *
     * @return the number of entries
     */
    long size();

    /**
     * Whether the map contains the key.
     *
     * @param key the key
     * @return true if the map contains an entry for this key
     */
    boolean containsKey(K key);

    /**
     * Whether the map is empty.
     *
     * @return true if the map is empty
     */
    boolean isEmpty();

    /**
     * Whether this is in-memory map, meaning that changes are not persisted.
     * 
     * @return whether this map is in-memory
     */
    boolean isInMemory();

    /**
     * Get a cursor to iterate over a number of keys and values.
     *
     * @param from the first key to return
     * @return the cursor
     */
    StorageMapCursor<K, V> cursor(K from);

    default StorageMapCursor<K, V> cursor() {
        return cursor((K) null);
    }

    default StorageMapCursor<K, V> cursor(IterationParameters<K> parameters) {
        return cursor(parameters.from);
    }

    /**
     * Remove all entries.
     */
    void clear();

    /**
     * Remove map.
     */
    void remove();

    /**
     * Whether the map is closed.
     *
     * @return true if the map is closed
     */
    boolean isClosed();

    /**
     * Close the map. Accessing the data is still possible (to allow concurrent reads),
     * but it is marked as closed.
     */
    void close();

    /**
     * Save the map data to disk.
     */
    void save();

    long getDiskSpaceUsed();

    long getMemorySpaceUsed();

    //////////////////// 以下是异步API， 默认用同步API实现 ////////////////////////////////

    default void get(K key, AsyncHandler<AsyncResult<V>> handler) {
        V v = get(key);
        handleAsyncResult(handler, v);
    }

    default void put(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        V v = put(key, value);
        handleAsyncResult(handler, v);
    }

    default void putIfAbsent(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        V v = putIfAbsent(key, value);
        handleAsyncResult(handler, v);
    }

    default K append(V value, AsyncHandler<AsyncResult<K>> handler) {
        K k = append(value);
        handleAsyncResult(handler, k);
        return k;
    }

    default void replace(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        Boolean b = replace(key, oldValue, newValue);
        handleAsyncResult(handler, b);
    }

    default void remove(K key, AsyncHandler<AsyncResult<V>> handler) {
        V v = remove(key);
        handleAsyncResult(handler, v);
    }

    static <R> void handleAsyncResult(AsyncHandler<AsyncResult<R>> handler, R result) {
        AsyncResult<R> ar = new AsyncResult<>();
        ar.setResult(result);
        handler.handle(ar);
    }

    ////////////////////// 以下是分布式API ////////////////////////////////

    Future<Object> get(Session session, Object key);

    Future<Object> put(Session session, Object key, Object value, StorageDataType valueType, boolean addIfAbsent);

    Future<Object> append(Session session, Object value, StorageDataType valueType);

    Future<Boolean> replace(Session session, Object key, Object oldValue, Object newValue, StorageDataType valueType);

    Future<Object> remove(Session session, Object key);

    void addLeafPage(PageKey pageKey, ByteBuffer page, boolean addPage);

    void removeLeafPage(PageKey pageKey);

    LeafPageMovePlan prepareMoveLeafPage(LeafPageMovePlan leafPageMovePlan);

    ByteBuffer readPage(PageKey pageKey);

    Map<List<String>, List<PageKey>> getNodeToPageKeyMap(K from, K to);
}
