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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.lealone.db.Session;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.type.StorageDataType;

public class DelegatedStorageMap<K, V> implements StorageMap<K, V> {

    protected volatile StorageMap<K, V> map;

    public DelegatedStorageMap(StorageMap<K, V> map) {
        this.map = map;
    }

    @Override
    public String getName() {
        return map.getName();
    }

    @Override
    public StorageDataType getKeyType() {
        return map.getKeyType();
    }

    @Override
    public StorageDataType getValueType() {
        return map.getValueType();
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        return map.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return map.putIfAbsent(key, value);
    }

    @Override
    public V remove(K key) {
        return map.remove(key);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return map.replace(key, oldValue, newValue);
    }

    @Override
    public K firstKey() {
        return map.firstKey();
    }

    @Override
    public K lastKey() {
        return map.lastKey();
    }

    @Override
    public K lowerKey(K key) {
        return map.lowerKey(key);
    }

    @Override
    public K floorKey(K key) {
        return map.floorKey(key);
    }

    @Override
    public K higherKey(K key) {
        return map.higherKey(key);
    }

    @Override
    public K ceilingKey(K key) {
        return map.ceilingKey(key);
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        return map.areValuesEqual(a, b);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public long sizeAsLong() {
        return map.sizeAsLong();
    }

    @Override
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean isInMemory() {
        return map.isInMemory();
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return map.cursor(from);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public void remove() {
        map.remove();
    }

    @Override
    public boolean isClosed() {
        return map.isClosed();
    }

    @Override
    public void close() {
        map.close();
    }

    @Override
    public void save() {
        map.save();
    }

    @Override
    public void transferTo(WritableByteChannel target, K firstKey, K lastKey) throws IOException {
        map.transferTo(target, firstKey, lastKey);
    }

    @Override
    public void transferFrom(ReadableByteChannel src) throws IOException {
        map.transferFrom(src);
    }

    @Override
    public Storage getStorage() {
        return map.getStorage();
    }

    @Override
    public K append(V value) {
        return map.append(value);
    }

    @Override
    public void addLeafPage(ByteBuffer splitKey, ByteBuffer page, boolean last, boolean addPage) {
        map.addLeafPage(splitKey, page, last, addPage);
    }

    @Override
    public void removeLeafPage(ByteBuffer key) {
        map.removeLeafPage(key);
    }

    @Override
    public List<NetEndpoint> getReplicationEndpoints(Object key) {
        return map.getReplicationEndpoints(key);
    }

    @Override
    public Object replicationPut(Session session, Object key, Object value, StorageDataType valueType) {
        return map.replicationPut(session, key, value, valueType);
    }

    @Override
    public Object replicationGet(Session session, Object key) {
        return map.replicationGet(session, key);
    }

    @Override
    public Object replicationAppend(Session session, Object value, StorageDataType valueType) {
        return map.replicationAppend(session, value, valueType);
    }

    @Override
    public LeafPageMovePlan prepareMoveLeafPage(LeafPageMovePlan leafPageMovePlan) {
        return map.prepareMoveLeafPage(leafPageMovePlan);
    }

    @Override
    public StorageMap<Object, Object> getRawMap() {
        return map.getRawMap();
    }

    @Override
    public ByteBuffer readPage(ByteBuffer key, boolean last) {
        return map.readPage(key, last);
    }

    @Override
    public void setRootPage(ByteBuffer buff) {
        map.setRootPage(buff);
    }
}
