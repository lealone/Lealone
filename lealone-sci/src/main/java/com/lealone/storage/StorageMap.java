/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage;

import java.util.concurrent.ConcurrentHashMap;

import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.async.Future;
import com.lealone.db.session.InternalSession;
import com.lealone.storage.type.StorageDataType;

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
        return null;
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
     * Get the number of entries.
     *
     * @return the number of entries
     */
    long size();

    default void decrementSize() {
    }

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
    default StorageMapCursor<K, V> cursor(K from) {
        return cursor(CursorParameters.create(from));
    }

    default StorageMapCursor<K, V> cursor() {
        return cursor(CursorParameters.create(null));
    }

    StorageMapCursor<K, V> cursor(CursorParameters<K> parameters);

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

    default void save(long dirtyMemory) {
    }

    default void repair() {
    }

    default long getDiskSpaceUsed() {
        return 0;
    }

    default long getMemorySpaceUsed() {
        return 0;
    }

    default boolean hasUnsavedChanges() {
        return false;
    }

    default boolean needGc() {
        return false;
    }

    default void gc() {
    }

    default void fullGc() {
    }

    default long collectDirtyMemory() {
        return 0;
    }

    //////////////////// 以下是异步API， 默认用同步API实现 ////////////////////////////////

    default void get(K key, AsyncHandler<AsyncResult<V>> handler) {
        V v = get(key);
        handleAsyncResult(handler, v);
    }

    default Future<Object> get(InternalSession session, K key) {
        Object v = get(key);
        return Future.succeededFuture(v);
    }

    default void put(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        V v = put(key, value);
        handleAsyncResult(handler, v);
    }

    default void put(InternalSession session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        put(key, value, handler);
    }

    default void putIfAbsent(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        V v = putIfAbsent(key, value);
        handleAsyncResult(handler, v);
    }

    default void putIfAbsent(InternalSession session, K key, V value,
            AsyncHandler<AsyncResult<V>> handler) {
        putIfAbsent(key, value, handler);
    }

    default void append(V value, AsyncHandler<AsyncResult<K>> handler) {
        K k = append(value);
        handleAsyncResult(handler, k);
    }

    default void append(InternalSession session, V value, AsyncHandler<AsyncResult<K>> handler) {
        append(value, handler);
    }

    default void remove(K key, AsyncHandler<AsyncResult<V>> handler) {
        V v = remove(key);
        handleAsyncResult(handler, v);
    }

    default void remove(InternalSession session, K key, AsyncHandler<AsyncResult<V>> handler) {
        remove(key, handler);
    }

    static <R> void handleAsyncResult(AsyncHandler<AsyncResult<R>> handler, R result) {
        handler.handle(new AsyncResult<>(result));
    }

    default ConcurrentHashMap<Object, Object> getOldValueCache() {
        return null;
    }
}
