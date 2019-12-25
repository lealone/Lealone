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
package org.lealone.client.storage;

import java.nio.ByteBuffer;

import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.StorageDataType;

public class ClientStorageMap<K, V> extends StorageMapBase<K, V> {

    private boolean closed;
    private final StorageCommand storageCommand;

    public ClientStorageMap(String name, StorageDataType keyType, StorageDataType valueType, ClientStorage storage) {
        super(name, keyType, valueType, storage);
        storageCommand = storage.getSession().createStorageCommand();
    }

    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public V put(K key, V value) {
        Object b = storageCommand.put(name, k2b(key), v2b(value), true, null);
        return b2v((ByteBuffer) b);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return null;
    }

    @Override
    public V remove(K key) {
        return null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return false;
    }

    @Override
    public K firstKey() {
        return null;
    }

    @Override
    public K lastKey() {
        return null;
    }

    @Override
    public K lowerKey(K key) { // 小于给定key的最大key
        return null;
    }

    @Override
    public K floorKey(K key) { // 小于或等于给定key的最大key
        return null;
    }

    @Override
    public K higherKey(K key) { // 大于给定key的最小key
        return null;
    }

    @Override
    public K ceilingKey(K key) { // 大于或等于给定key的最小key
        return null;
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return valueType.compare(a, b) == 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public boolean containsKey(K key) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean isInMemory() {
        return true;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return new ClientStorageMapCursor<>(null);
    }

    @Override
    public void clear() {
    }

    @Override
    public void remove() {
        clear();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        clear();
        closed = true;
    }

    @Override
    public void save() {
    }

    private ByteBuffer k2b(K k) {
        try (DataBuffer buff = DataBuffer.create()) {
            ByteBuffer keyBuffer = buff.write(keyType, k);
            return keyBuffer;
        }
    }

    private ByteBuffer v2b(V v) {
        try (DataBuffer buff = DataBuffer.create()) {
            ByteBuffer valueBuffer = buff.write(valueType, v);
            return valueBuffer;
        }
    }

    @SuppressWarnings("unchecked")
    private V b2v(ByteBuffer b) {
        return (V) valueType.read(b);
    }
}
