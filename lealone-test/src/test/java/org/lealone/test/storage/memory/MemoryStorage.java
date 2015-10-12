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
package org.lealone.test.storage.memory;

import java.util.Map;

import org.lealone.common.util.BitField;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapBuilder;
import org.lealone.storage.type.DataType;

public class MemoryStorage implements Storage {

    @Override
    public <M extends StorageMap<K, V>, K, V> M openMap(String name, StorageMapBuilder<M, K, V> builder) {
        // TODO Auto-generated method stub
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M extends StorageMap<K, V>, K, V> StorageMapBuilder<M, K, V> getStorageMapBuilder(String type) {
        return (StorageMapBuilder<M, K, V>) new MemoryMapBuilder<>();
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void backupTo(String fileName) {
        // TODO Auto-generated method stub

    }

    @Override
    public void flush() {
        // TODO Auto-generated method stub

    }

    @Override
    public void sync() {
        // TODO Auto-generated method stub

    }

    @Override
    public void initTransactions() {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeTemporaryMaps(BitField objectIds) {
        // TODO Auto-generated method stub

    }

    @Override
    public void closeImmediately() {
        // TODO Auto-generated method stub

    }

    @Override
    public String nextTemporaryMapName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasMap(String name) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, String mapType, DataType keyType, DataType valueType,
            Map<String, String> parameters) {
        // TODO Auto-generated method stub
        return null;
    }
}
