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

import org.lealone.storage.StorageBase;
import org.lealone.storage.type.StorageDataType;

public class MemoryStorage extends StorageBase {

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> MemoryMap<K, V> openMap(String name, String mapType, StorageDataType keyType, StorageDataType valueType,
            Map<String, String> parameters) {
        MemoryMap<K, V> map = (MemoryMap<K, V>) maps.get(name);
        if (map == null) {
            synchronized (this) {
                map = (MemoryMap<K, V>) maps.get(name);
                if (map == null) {
                    map = new MemoryMap<>(name, keyType, valueType);
                    maps.put(name, map);
                    map.setMemoryStorage(this);
                }
            }
        }
        return map;
    }

}
