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
package org.lealone.storage.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.util.BitField;
import org.lealone.db.Constants;
import org.lealone.storage.Storage;
import org.lealone.storage.type.DataType;

public class MemoryStorage implements Storage {
    private static final AtomicInteger counter = new AtomicInteger(0);

    private final ConcurrentHashMap<String, MemoryMap<?, ?>> maps = new ConcurrentHashMap<>();

    @Override
    public void close() {
        for (MemoryMap<?, ?> map : maps.values())
            map.close();
        maps.clear();
    }

    @Override
    public void closeImmediately() {
        close();
    }

    @Override
    public void backupTo(String fileName) {
    }

    @Override
    public void flush() {
    }

    @Override
    public void sync() {
    }

    @Override
    public void removeTemporaryMaps(BitField objectIds) {
    }

    @Override
    public String nextTemporaryMapName() {
        int i = 0;
        String name = null;
        while (true) {
            name = "temp" + Constants.NAME_SEPARATOR + i;
            if (!maps.containsKey(name))
                return name;
        }
    }

    @Override
    public boolean hasMap(String name) {
        return maps.containsKey(name);
    }

    @Override
    public <K, V> MemoryMap<K, V> openMap(String name, String mapType, DataType keyType, DataType valueType,
            Map<String, String> parameters) {
        int id = counter.incrementAndGet();
        MemoryMap<K, V> map = new MemoryMap<>(id, name, keyType, valueType);
        maps.put(name, map);
        return map;
    }
}
