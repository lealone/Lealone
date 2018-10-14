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
package org.lealone.storage.aose;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.db.value.ValueString;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.StorageBase;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMap;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.rtree.RTreeMap;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.replication.ReplicationSession;
import org.lealone.storage.type.StorageDataType;

/**
 * Adaptive optimization storage
 * 
 * @author zhh
 */
public class AOStorage extends StorageBase {

    public static final String SUFFIX_AO_FILE = ".db";
    public static final int SUFFIX_AO_FILE_LENGTH = SUFFIX_AO_FILE.length();

    private final IDatabase db;

    AOStorage(Map<String, Object> config) {
        super(config);
        this.db = (IDatabase) config.get("db");
        String storagePath = getStoragePath();
        DataUtils.checkArgument(storagePath != null, "The storage path may not be null");
        if (!FileUtils.exists(storagePath))
            FileUtils.createDirectories(storagePath);
        FilePath dir = FilePath.get(storagePath);
        for (FilePath fp : dir.newDirectoryStream()) {
            String mapFullName = fp.getName();
            if (mapFullName.startsWith(TEMP_NAME_PREFIX)) {
                fp.delete();
            }
        }
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, String> parameters) {
        String mapType = parameters == null ? null : parameters.get("mapType");
        return openMap(name, mapType, keyType, valueType, parameters);
    }

    public <K, V> StorageMap<K, V> openMap(String name, String mapType, StorageDataType keyType,
            StorageDataType valueType, Map<String, String> parameters) {
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

    public <K, V> BTreeMap<K, V> openBTreeMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, String> parameters) {
        BTreeMap.Builder<K, V> builder = new BTreeMap.Builder<>();
        builder.keyType(keyType);
        builder.valueType(valueType);
        return openMap(name, builder, parameters);
    }

    public <V> RTreeMap<V> openRTreeMap(String name, StorageDataType valueType, int dimensions) {
        RTreeMap.Builder<V> builder = new RTreeMap.Builder<>();
        builder.dimensions(dimensions);
        builder.valueType(valueType);
        return openMap(name, builder, null);
    }

    public <K, V> AOMap<K, V> openAOMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, String> parameters) {
        BTreeMap<K, V> btreeMap = openBTreeMap(name, keyType, valueType, parameters);
        AOMap<K, V> map = new AOMap<>(btreeMap);
        maps.put(name, map); // 覆盖btreeMap
        AOStorageService.addAOMap(map);
        return map;
    }

    public <K, V> BufferedMap<K, V> openBufferedMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, String> parameters) {
        BTreeMap<K, V> btreeMap = openBTreeMap(name, keyType, valueType, parameters);
        BufferedMap<K, V> map = new BufferedMap<>(btreeMap);
        maps.put(name, map); // 覆盖btreeMap
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
                    CaseInsensitiveMap<Object> c = new CaseInsensitiveMap<>(config);
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

    boolean isReadOnly() {
        return config.containsKey("readOnly");
    }

    @Override
    public void save() {
        AOStorageService.forceMerge();
        super.save();
    }

    private List<NetEndpoint> getReplicationEndpoints(String[] replicationHostIds) {
        return getReplicationEndpoints(Arrays.asList(replicationHostIds));
    }

    private List<NetEndpoint> getReplicationEndpoints(List<String> replicationHostIds) {
        int size = replicationHostIds.size();
        List<NetEndpoint> replicationEndpoints = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            replicationEndpoints.add(db.getEndpoint(replicationHostIds.get(i)));
        }
        return replicationEndpoints;
    }

    @Override
    public void replicate(Object dbObject, String[] newReplicationEndpoints, RunMode runMode) {
        replicateRootPages(dbObject, null, newReplicationEndpoints, runMode);
    }

    @Override
    public void sharding(Object dbObject, String[] oldEndpoints, String[] newEndpoints, RunMode runMode) {
        replicateRootPages(dbObject, oldEndpoints, newEndpoints, runMode);
    }

    private void replicateRootPages(Object dbObject, String[] oldEndpoints, String[] targetEndpoints, RunMode runMode) {
        AOStorageService.forceMerge();

        List<NetEndpoint> replicationEndpoints = getReplicationEndpoints(targetEndpoints);
        // 用最高权限的用户移动页面，因为目标节点上可能还没有对应的数据库
        IDatabase db = (IDatabase) dbObject;
        Session session = db.createInternalSession(true);
        ReplicationSession rs = db.createReplicationSession(session, replicationEndpoints);
        int id = db.getId();
        String sysMapName = "t_" + id + "_0";
        try (DataBuffer p = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
            HashMap<String, StorageMap<?, ?>> maps = new HashMap<>(this.maps);
            Collection<StorageMap<?, ?>> values = maps.values();
            p.putInt(values.size());
            // SYS表放在前面，并且总是使用CLIENT_SERVER模式
            StorageMap<?, ?> sysMap = maps.remove(sysMapName);
            replicateRootPage(db, sysMap, p, oldEndpoints, RunMode.CLIENT_SERVER);
            for (StorageMap<?, ?> map : values) {
                replicateRootPage(db, map, p, oldEndpoints, runMode);
            }
            ByteBuffer pageBuffer = p.getAndFlipBuffer();
            c.replicateRootPages(db.getShortName(), pageBuffer);
            db.notifyRunModeChanged();
        }
    }

    private void replicateRootPage(IDatabase db, StorageMap<?, ?> map, DataBuffer p, String[] oldEndpoints,
            RunMode runMode) {
        map = map.getRawMap();
        if (map instanceof BTreeMap) {
            String mapName = map.getName();
            ValueString.type.write(p, mapName);

            BTreeMap<?, ?> btreeMap = (BTreeMap<?, ?>) map;
            btreeMap.setOldEndpoints(oldEndpoints);
            btreeMap.setDatabase(db);
            btreeMap.setRunMode(runMode);
            btreeMap.replicateRootPage(p);
        }
    }

    @Override
    public void scaleIn(Object dbObject, RunMode oldRunMode, RunMode newRunMode, String[] oldEndpoints,
            String[] newEndpoints) {
        IDatabase db = (IDatabase) dbObject;
        for (StorageMap<?, ?> map : maps.values()) {
            map = map.getRawMap();
            if (map instanceof BTreeMap) {
                BTreeMap<?, ?> btreeMap = (BTreeMap<?, ?>) map;
                btreeMap.setOldEndpoints(oldEndpoints);
                btreeMap.setDatabase(db);
                btreeMap.setRunMode(newRunMode);
                if (oldEndpoints == null) {
                    btreeMap.replicateAllRemotePages();
                } else {
                    btreeMap.moveAllLocalLeafPages(oldEndpoints, newEndpoints);
                }
            }
        }
        db.notifyRunModeChanged();
    }
}
