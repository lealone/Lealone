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
package org.lealone.storage;

import org.lealone.util.DataUtils;

@SuppressWarnings("unchecked")
public class WTCursor<K, V> implements org.lealone.storage.StorageMap.Cursor<K, V> {

    private final com.wiredtiger.db.Cursor wtCursor;
    private final WTMap<K, V> map;
    private K from;
    private K key;
    private V value;

    public WTCursor(com.wiredtiger.db.Cursor wtCursor, WTMap<K, V> map, K from) {
        this.wtCursor = wtCursor;
        this.map = map;
        this.from = from;
        wtCursor.reset();
        if (from != null) {
            map.putWTKey(from);
        }
    }

    @Override
    public boolean hasNext() {
        if (from != null) {
            from = null;
            if (wtCursor.search() == 0) {
                key = (K) map.getWTKey();
                value = (V) map.getWTValue();
                return true;
            }
            return false;
        }
        if (wtCursor.next() == 0) {
            key = (K) map.getWTKey();
            value = (V) map.getWTValue();
            return true;
        }
        return false;
    }

    @Override
    public K next() {
        return key;
    }

    /**
     * Get the last read key if there was one.
     *
     * @return the key or null
     */
    @Override
    public K getKey() {
        return key;
    }

    /**
     * Get the last read value if there was one.
     *
     * @return the value or null
     */
    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void remove() {
        throw DataUtils.newUnsupportedOperationException("Removing is not supported");
    }
}
