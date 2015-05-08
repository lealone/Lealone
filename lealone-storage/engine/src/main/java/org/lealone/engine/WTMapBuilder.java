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
package org.lealone.engine;

import org.lealone.engine.StorageMap;
import org.lealone.type.DataType;
import org.lealone.type.ObjectDataType;

public class WTMapBuilder implements org.lealone.engine.StorageMap.Builder {
    private final com.wiredtiger.db.Session wtSession;

    public WTMapBuilder(com.wiredtiger.db.Session wtSession) {
        this.wtSession = wtSession;
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name) {
        return openMap(name, null, null);
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, DataType valueType) {
        return openMap(name, null, valueType);
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, DataType keyType, DataType valueType) {
        if (keyType == null) {
            keyType = new ObjectDataType();
        }
        if (valueType == null) {
            valueType = new ObjectDataType();
        }
        return new WTMap<K, V>(wtSession, name, keyType, valueType);
    }
}
