/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncPeriodicTask;
import org.lealone.db.async.AsyncTaskHandler;

public class ExpiringMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ExpiringMap.class);

    public static class CacheableObject<T> {
        public final T value;
        public final long timeout;
        private final long createdAt;
        private long last;

        private CacheableObject(T value, long timeout) {
            assert value != null;
            this.value = value;
            this.timeout = timeout;
            last = createdAt = System.nanoTime();
        }

        private boolean isReadyToDieAt(long atNano) {
            return atNano - last > TimeUnit.MILLISECONDS.toNanos(timeout);
        }
    }

    private final Map<K, CacheableObject<V>> cache;
    private final long defaultExpiration;
    private final AsyncTaskHandler asyncTaskHandler;
    private final AsyncPeriodicTask task;

    /**
     *
     * @param defaultExpiration the TTL for objects in the cache in milliseconds
     */
    public ExpiringMap(AsyncTaskHandler asyncTaskHandler, long defaultExpiration, boolean isThreadSafe,
            final Function<ExpiringMap.CacheableObject<V>, ?> postExpireHook) {
        // if (defaultExpiration <= 0) {
        // throw new IllegalArgumentException("Argument specified must be a positive number");
        // }
        if (isThreadSafe)
            cache = new HashMap<>();
        else
            cache = new ConcurrentHashMap<>();
        this.defaultExpiration = defaultExpiration;
        this.asyncTaskHandler = asyncTaskHandler;
        task = new AsyncPeriodicTask(1000, () -> {
            long start = System.nanoTime();
            int n = 0;
            for (Map.Entry<K, CacheableObject<V>> entry : cache.entrySet()) {
                if (entry.getValue().isReadyToDieAt(start)) {
                    if (cache.remove(entry.getKey()) != null) {
                        n++;
                        if (postExpireHook != null)
                            postExpireHook.apply(entry.getValue());
                    }
                }
            }
            if (logger.isTraceEnabled())
                logger.trace("Expired {} entries", n);
        });
        if (defaultExpiration > 0)
            asyncTaskHandler.addPeriodicTask(task);
    }

    public AsyncPeriodicTask getAsyncPeriodicTask() {
        return task;
    }

    public void reset() {
        cache.clear();
    }

    public void close() {
        for (CacheableObject<V> c : cache.values()) {
            if (c.value instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) c.value).close();
                } catch (Throwable t) {
                    // ignore
                }
            }
        }
        cache.clear();
        asyncTaskHandler.removePeriodicTask(task);
    }

    public V put(K key, V value) {
        return put(key, value, defaultExpiration);
    }

    public V put(K key, V value, long timeout) {
        CacheableObject<V> previous = cache.put(key, new CacheableObject<V>(value, timeout));
        return (previous == null) ? null : previous.value;
    }

    public V get(K key) {
        return get(key, false);
    }

    public V remove(K key) {
        return remove(key, false);
    }

    /**
     * @return System.nanoTime() when key was put into the map.
     */
    public long getAge(K key) {
        CacheableObject<V> co = cache.get(key);
        return co == null ? 0 : co.createdAt;
    }

    public int size() {
        return cache.size();
    }

    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    public Set<K> keySet() {
        return cache.keySet();
    }

    /**
     * Get an object from the map if it is stored.
     *
     * @param key the key of the object
     * @param ifAvailable only return it if available, otherwise return null
     * @return the object or null
     * @throws DbException if isAvailable is false and the object has not been found
     */
    public V get(K key, boolean ifAvailable) {
        CacheableObject<V> co = cache.get(key);
        return getValue(co, ifAvailable);
    }

    /**
     * Remove an object from the map.
     *
     * @param key the key of the object
     * @param ifAvailable only return it if available, otherwise return null
     * @return the object or null
     * @throws DbException if isAvailable is false and the object has not been found
     */
    public V remove(K key, boolean ifAvailable) {
        CacheableObject<V> co = cache.remove(key);
        return getValue(co, ifAvailable);
    }

    private V getValue(CacheableObject<V> co, boolean ifAvailable) {
        if (co == null) {
            if (!ifAvailable) {
                throw DbException.get(ErrorCode.OBJECT_CLOSED);
            }
            return null;
        } else {
            co.last = System.nanoTime();
            return co.value;
        }
    }
}
