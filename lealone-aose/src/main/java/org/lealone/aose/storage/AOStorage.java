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
package org.lealone.aose.storage;

import java.util.HashMap;
import java.util.Map;

import org.lealone.aose.storage.btree.BTreeMap;
import org.lealone.aose.storage.rtree.RTreeMap;
import org.lealone.common.util.DataUtils;
import org.lealone.storage.StorageBase;
import org.lealone.storage.StorageMap;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.type.DataType;

/**
 * Adaptive optimization storage
 * 
 * @author zhh
 */
public class AOStorage extends StorageBase {

    public static final String SUFFIX_AO_FILE = ".db";
    public static final int SUFFIX_AO_FILE_LENGTH = SUFFIX_AO_FILE.length();

    private final Map<String, Object> config;

    AOStorage(Map<String, Object> config) {
        this.config = config;
        String storageName = (String) config.get("storageName");
        DataUtils.checkArgument(storageName != null, "The storage name may not be null");
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

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, String mapType, DataType keyType, DataType valueType,
            Map<String, String> parameters) {
        if (mapType == null || mapType.equalsIgnoreCase("AOMap")) {
            return openAOMap(name, keyType, valueType, parameters);
        } else if (mapType.equalsIgnoreCase("BTreeMap")) {
            return openBTreeMap(name, keyType, valueType, parameters);
        } else if (mapType.equalsIgnoreCase("BufferedMap")) {
            return openBufferedMap(name, keyType, valueType, parameters);
        } else {
            throw DataUtils.newIllegalArgumentException("Unknow map type: {0}", mapType);
        }
    }

    public <K, V> BTreeMap<K, V> openBTreeMap(String name) {
        return openBTreeMap(name, null, null, null);
    }

    public <K, V> BTreeMap<K, V> openBTreeMap(String name, DataType keyType, DataType valueType,
            Map<String, String> parameters) {
        BTreeMap.Builder<K, V> builder = new BTreeMap.Builder<>();
        builder.keyType(keyType);
        builder.valueType(valueType);
        return openMap(name, builder, parameters);
    }

    public <V> RTreeMap<V> openRTreeMap(String name, DataType valueType, int dimensions) {
        RTreeMap.Builder<V> builder = new RTreeMap.Builder<>();
        builder.dimensions(dimensions);
        builder.valueType(valueType);
        return openMap(name, builder, null);
    }

    public <K, V> AOMap<K, V> openAOMap(String name, DataType keyType, DataType valueType,
            Map<String, String> parameters) {
        BTreeMap<K, V> btreeMap = openBTreeMap(name, keyType, valueType, parameters);
        AOMap<K, V> map = new AOMap<>(btreeMap);
        AOStorageService.addAOMap(map);
        return map;
    }

    public <K, V> BufferedMap<K, V> openBufferedMap(String name, DataType keyType, DataType valueType,
            Map<String, String> parameters) {
        BTreeMap<K, V> btreeMap = openBTreeMap(name, keyType, valueType, parameters);
        BufferedMap<K, V> map = new BufferedMap<>(btreeMap);
        AOStorageService.addBufferedMap(map);
        return map;
    }

    @SuppressWarnings("unchecked")
    private <M extends StorageMap<K, V>, K, V> M openMap(String name, StorageMapBuilder<M, K, V> builder,
            Map<String, String> parameters) {
        M map = (M) maps.get(name);
        if (map == null) {
            synchronized (this) {
                map = (M) maps.get(name);
                if (map == null) {
                    HashMap<String, Object> c = new HashMap<>(config);
                    if (parameters != null)
                        c.putAll(parameters);
                    builder.name(name).config(c).aoStorage(this);
                    map = builder.openMap();
                    maps.put(name, map);
                }
            }
        }
        return map;
    }

    public boolean isReadOnly() {
        return config.containsKey("readOnly");
    }

    @Override
    public void backupTo(String fileName) {
        // TODO Auto-generated method stub
    }

}
