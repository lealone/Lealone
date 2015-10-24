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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.db.Constants;
import org.lealone.storage.btree.BTreeMap;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.memory.MemoryMap;
import org.lealone.storage.rtree.RTreeMap;
import org.lealone.storage.type.DataType;

/**
 * Adaptive optimization storage
 * 
 * @author zhh
 */
public class AOStorage implements Storage {
    // /**
    // * The file name suffix of a new AOStorage file, used when compacting a storage.
    // */
    // public static final String SUFFIX_AO_STORE_NEW_FILE = ".newFile";
    //
    // /**
    // * The file name suffix of a temporary AOStorage file, used when compacting a storage.
    // */
    // public static final String SUFFIX_AO_STORE_TEMP_FILE = ".tempFile";

    public static final String SUFFIX_AO_FILE = ".db";
    public static final int SUFFIX_AO_FILE_LENGTH = SUFFIX_AO_FILE.length();

    private static final String TEMP_NAME_PREFIX = "temp" + Constants.NAME_SEPARATOR;

    private final ConcurrentHashMap<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();
    private final Map<String, Object> config;

    private boolean closed;
    private int nextTemporaryMapId;

    AOStorage(Map<String, Object> config) {
        this.config = config;
        if (!config.containsKey("inMemory")) {
            String storageName = (String) config.get("storageName");
            if (storageName != null) {
                if (!FileUtils.exists(storageName))
                    FileUtils.createDirectories(storageName);
                FilePath dir = FilePath.get(storageName);
                for (FilePath fp : dir.newDirectoryStream()) {
                    String mapFullName = fp.getName();
                    if (mapFullName.startsWith(TEMP_NAME_PREFIX)) {
                        fp.delete();
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public synchronized <M extends StorageMap<K, V>, K, V> M openMap(String name, StorageMapBuilder<M, K, V> builder) {
        M map = (M) maps.get(name);
        if (map == null) {
            HashMap<String, Object> c = new HashMap<>(config);
            builder.name(name).config(c);
            map = builder.openMap();
            maps.put(name, map);
        }

        return map;
    }

    public <K, V> BTreeMap<K, V> openBTreeMap(String name) {
        return openBTreeMap(name, null, null);
    }

    public <K, V> BTreeMap<K, V> openBTreeMap(String name, DataType keyType, DataType valueType) {
        BTreeMap.Builder<K, V> builder = new BTreeMap.Builder<>();
        builder.keyType(keyType);
        builder.valueType(valueType);
        return openMap(name, builder);
    }

    public <V> RTreeMap<V> openRTreeMap(String name, DataType valueType, int dimensions) {
        RTreeMap.Builder<V> builder = new RTreeMap.Builder<>();
        builder.dimensions(dimensions);
        builder.valueType(valueType);
        return openMap(name, builder);
    }

    public <K, V> AOMap<K, V> openAOMap(String name, DataType keyType, DataType valueType) {
        BTreeMap<K, V> btreeMap = openBTreeMap(name, keyType, valueType);
        AOMap<K, V> map = new AOMap<>(btreeMap);
        AOStorageService.addAOMap(map);
        return map;
    }

    public <K, V> BufferedMap<K, V> openBufferedMap(String name, DataType keyType, DataType valueType) {
        BTreeMap<K, V> btreeMap = openBTreeMap(name, keyType, valueType);
        BufferedMap<K, V> map = new BufferedMap<>(btreeMap);
        AOStorageService.addBufferedMap(map);
        return map;
    }

    public <K, V> MemoryMap<K, V> openMemoryMap(String name, DataType keyType, DataType valueType) {
        MemoryMapBuilder<K, V> builder = new MemoryMapBuilder<>();
        builder.keyType(keyType);
        builder.valueType(valueType);
        return openMap(name, builder);
    }

    private static class MemoryMapBuilder<K, V> extends StorageMapBuilder<MemoryMap<K, V>, K, V> {
        @Override
        public MemoryMap<K, V> openMap() {
            return new MemoryMap<>(name, keyType, valueType);
        }
    }

    public boolean isReadOnly() {
        return config.containsKey("readOnly");
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void close() {
        close(true);
    }

    @Override
    public void closeImmediately() {
        close(false);
    }

    private void close(boolean closeMaps) {
        closed = true;

        for (StorageMap<?, ?> map : maps.values())
            map.close();

        maps.clear();
    }

    public synchronized void commit() {
        for (StorageMap<?, ?> map : maps.values())
            map.save();
    }

    public Set<String> getMapNames() {
        return new HashSet<String>(maps.keySet());
    }

    public Collection<StorageMap<?, ?>> getMaps() {
        return maps.values();
    }

    @Override
    public boolean hasMap(String name) {
        return maps.containsKey(name);
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, String mapType, DataType keyType, DataType valueType,
            Map<String, String> parameters) {
        if (mapType == null || mapType.equalsIgnoreCase("AOMap)")) {
            return openAOMap(name, keyType, valueType);
        } else if (mapType.equalsIgnoreCase("BTreeMap)")) {
            return openBTreeMap(name, keyType, valueType);
        } else if (mapType.equalsIgnoreCase("BufferedMap)")) {
            return openBufferedMap(name, keyType, valueType);
        } else if (mapType.equalsIgnoreCase("MemoryMap)")) {
            return openMemoryMap(name, keyType, valueType);
        } else {
            return openAOMap(name, keyType, valueType);
        }
    }

    @Override
    public void backupTo(String fileName) {
        // TODO Auto-generated method stub
    }

    @Override
    public void flush() {
        commit();
    }

    @Override
    public void sync() {
        commit();
    }

    @Override
    public synchronized String nextTemporaryMapName() {
        return TEMP_NAME_PREFIX + nextTemporaryMapId++;
    }
}
