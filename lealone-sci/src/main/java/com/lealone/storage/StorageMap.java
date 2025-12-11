/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import com.lealone.db.DataBuffer;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.async.Future;
import com.lealone.db.lock.Lockable;
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

    default void gc() {
    }

    default void fullGc() {
    }

    default long collectDirtyMemory() {
        return 0;
    }

    //////////////////// 以下是异步API， 默认用同步API实现 ////////////////////////////////

    default void get(K key, AsyncResultHandler<V> handler) {
        V v = get(key);
        handleAsyncResult(handler, v);
    }

    default Future<Object> get(InternalSession session, K key) {
        Object v = get(key);
        return Future.succeededFuture(v);
    }

    default void put(K key, V value, AsyncResultHandler<V> handler) {
        V v = put(key, value);
        handleAsyncResult(handler, v);
    }

    default void put(InternalSession session, K key, V value, AsyncResultHandler<V> handler) {
        put(key, value, handler);
    }

    default void putIfAbsent(K key, V value, AsyncResultHandler<V> handler) {
        V v = putIfAbsent(key, value);
        handleAsyncResult(handler, v);
    }

    default void putIfAbsent(InternalSession session, K key, V value, AsyncResultHandler<V> handler) {
        putIfAbsent(key, value, handler);
    }

    default void append(V value, AsyncResultHandler<K> handler) {
        K k = append(value);
        handleAsyncResult(handler, k);
    }

    default void append(InternalSession session, V value, AsyncResultHandler<K> handler) {
        append(value, handler);
    }

    default void remove(K key, AsyncResultHandler<V> handler) {
        V v = remove(key);
        handleAsyncResult(handler, v);
    }

    default void remove(InternalSession session, K key, AsyncResultHandler<V> handler) {
        remove(key, handler);
    }

    static <R> void handleAsyncResult(AsyncResultHandler<R> handler, R result) {
        handler.handleResult(result);
    }

    default ConcurrentHashMap<Lockable, Object> getOldValueCache() {
        return null;
    }

    //////////////////// 以下是RedoLog相关API ////////////////////////////////

    default void writeRedoLog(ByteBuffer log) {
    }

    default ByteBuffer readRedoLog() {
        return null;
    }

    default void sync() {
    }

    default int getRedoLogServiceIndex() {
        return -1;
    }

    default void setRedoLogServiceIndex(int index) {
    }

    default RedoLogBuffer getRedoLogBuffer() {
        return null;
    }

    default void setRedoLogBuffer(RedoLogBuffer redoLogBuffer) {
    }

    default long getLastTransactionId() {
        return -1;
    }

    default void setLastTransactionId(long lastTransactionId) {
    }

    default boolean validateRedoLog(long lastTransactionId) {
        return true;
    }

    // 只有一个线程访问
    public static class RedoLogBuffer {

        private final StorageMap<?, ?> map;
        private DataBuffer log;
        private long lastSyncedAt = System.currentTimeMillis();

        public RedoLogBuffer(StorageMap<?, ?> map) {
            this.map = map;
        }

        public StorageMap<?, ?> getMap() {
            return map;
        }

        public DataBuffer getLog() {
            if (log == null)
                log = DataBuffer.createDirect();
            return log;
        }

        public void sync() {
            map.sync();
            lastSyncedAt = System.currentTimeMillis();
        }

        public int writeRedoLog() {
            if (log.length() == 0)
                return 0;
            ByteBuffer buffer = log.getAndFlipBuffer();
            int length = buffer.limit();
            map.writeRedoLog(buffer);
            log.clear();
            return length;
        }

        public void clearIdleBuffer(long now, long maxIdleTime) {
            if (log != null && lastSyncedAt + maxIdleTime < now) {
                log = null;
            }
        }
    }
}
