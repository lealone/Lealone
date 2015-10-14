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
package org.lealone.transaction.log;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;

/**
 * A log map
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author zhh
 */
public class LogMap<K, V> implements StorageMap<K, V> {

    private static final long DEFAULT_LOG_CHUNK_SIZE = 32 * 1024 * 1024;

    private final CopyOnWriteArrayList<LogChunkMap<K, V>> chunks = new CopyOnWriteArrayList<>();
    private LogChunkMap<K, V> current;

    private int id;
    private final String name;
    private final DataType keyType;
    private final DataType valueType;
    private final Map<String, String> config;
    private final long logChunkSize;

    public LogMap(int id, String name, DataType keyType, DataType valueType, Map<String, String> config) {
        if (keyType == null)
            keyType = new ObjectDataType();
        if (valueType == null)
            valueType = new ObjectDataType();

        this.id = id;
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        this.config = config;

        current = new LogChunkMap<>(id, name, keyType, valueType, config);
        if (config.containsKey("log_chunk_size"))
            logChunkSize = Long.parseLong(config.get("log_chunk_size"));
        else
            logChunkSize = DEFAULT_LOG_CHUNK_SIZE;
    }

    @Override
    public int getId() {
        return current.getId();
    }

    @Override
    public String getName() {
        return current.getName();
    }

    @Override
    public DataType getKeyType() {
        return current.getKeyType();
    }

    @Override
    public DataType getValueType() {
        return current.getValueType();
    }

    @Override
    public V get(K key) {
        V v = current.get(key);
        if (v == null && chunks.isEmpty()) {
            // TODO read old
            LogChunkMap<K, V> chunk = new LogChunkMap<>(id, name, keyType, valueType, config);
            chunks.add(chunk);
            v = chunks.get(0).get(key);
        }
        return v;
    }

    @Override
    public V put(K key, V value) {
        return current.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return current.putIfAbsent(key, value);
    }

    @Override
    public V remove(K key) {
        return current.remove(key);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return current.replace(key, oldValue, newValue);
    }

    @Override
    public K firstKey() {
        return current.firstKey();
    }

    @Override
    public K lastKey() {
        return current.lastKey();
    }

    @Override
    public K lowerKey(K key) {
        return current.lowerKey(key);
    }

    @Override
    public K floorKey(K key) {
        return current.floorKey(key);
    }

    @Override
    public K higherKey(K key) {
        return current.higherKey(key);
    }

    @Override
    public K ceilingKey(K key) {
        return current.ceilingKey(key);
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        return current.areValuesEqual(a, b);
    }

    @Override
    public long size() {
        return current.size();
    }

    @Override
    public boolean containsKey(K key) {
        return current.containsKey(key);
    }

    @Override
    public boolean isEmpty() {
        return current.isEmpty();
    }

    @Override
    public boolean isInMemory() {
        return current.isInMemory();
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return current.cursor(from);
    }

    @Override
    public void clear() {
        current.clear();
    }

    @Override
    public void remove() {
        current.remove();
        LogStorage.logMaps.remove(this);
    }

    @Override
    public boolean isClosed() {
        return current.isClosed();
    }

    @Override
    public void close() {
        current.close();
    }

    @Override
    public void save() {
        current.save();
        if (current.logChunkSize() > logChunkSize) {
            current.close();
            current = new LogChunkMap<>(++id, name, keyType, valueType, config);
        }
    }

    public Set<Entry<K, V>> entrySet() {
        return current.entrySet();
    }

    public K getLastSyncKey() {
        return current.getLastSyncKey();
    }
}
