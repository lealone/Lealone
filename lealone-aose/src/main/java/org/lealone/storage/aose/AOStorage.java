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
import org.lealone.net.NetNode;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.StorageBase;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMap;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.BTreeMapBuilder;
import org.lealone.storage.aose.btree.DistributedBTreeMap;
import org.lealone.storage.aose.rtree.RTreeMap;
import org.lealone.storage.aose.rtree.RTreeMapBuilder;
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
    private final PageOperationHandlerFactory pohFactory;

    AOStorage(Map<String, Object> config, PageOperationHandlerFactory pohFactory) {
        super(config);
        this.db = (IDatabase) config.get("db");
        this.pohFactory = pohFactory;
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

    public PageOperationHandlerFactory getPageOperationHandlerFactory() {
        return pohFactory;
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, String> parameters) {
        String mapType = parameters == null ? null : parameters.get("mapType");
        return openMap(name, mapType, keyType, valueType, parameters);
    }

    public <K, V> StorageMap<K, V> openMap(String name, String mapType, StorageDataType keyType,
            StorageDataType valueType, Map<String, String> parameters) {
        if (isDistributed(parameters)) {
            return openDistributedBTreeMap(name, keyType, valueType, parameters);
        } else if (mapType == null || mapType.equalsIgnoreCase("BTreeMap")) {
            return openBTreeMap(name, keyType, valueType, parameters);
        } else {
            throw DataUtils.newIllegalArgumentException("Unknow map type: {0}", mapType);
        }
    }

    private boolean isDistributed(Map<String, String> parameters) {
        if (parameters != null && parameters.containsKey("isDistributed"))
            return Boolean.parseBoolean(parameters.get("isDistributed"));
        else
            return false;
    }

    public <K, V> BTreeMap<K, V> openBTreeMap(String name) {
        return openBTreeMap(name, null, null, null);
    }

    public <K, V> BTreeMap<K, V> openBTreeMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, String> parameters) {
        BTreeMapBuilder<K, V> builder = new BTreeMapBuilder<>();
        builder.keyType(keyType);
        builder.valueType(valueType);
        return openMap(name, builder, parameters);
    }

    public <K, V> DistributedBTreeMap<K, V> openDistributedBTreeMap(String name, StorageDataType keyType,
            StorageDataType valueType, Map<String, String> parameters) {
        DistributedBTreeMap.Builder<K, V> builder = new DistributedBTreeMap.Builder<>();
        builder.keyType(keyType);
        builder.valueType(valueType);
        return (DistributedBTreeMap<K, V>) openMap(name, builder, parameters);
    }

    public <V> RTreeMap<V> openRTreeMap(String name, StorageDataType valueType, int dimensions) {
        RTreeMapBuilder<V> builder = new RTreeMapBuilder<>();
        builder.dimensions(dimensions);
        builder.valueType(valueType);
        return (RTreeMap<V>) openMap(name, builder, null);
    }

    @SuppressWarnings("unchecked")
    private <K, V> BTreeMap<K, V> openMap(String name, BTreeMapBuilder<K, V> builder, Map<String, String> parameters) {
        StorageMap<?, ?> map = maps.get(name);
        if (map == null) {
            synchronized (this) {
                map = maps.get(name);
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
        return (BTreeMap<K, V>) map;
    }

    public boolean isReadOnly() {
        return config.containsKey("readOnly");
    }

    private List<NetNode> getReplicationNodes(String[] replicationHostIds) {
        return getReplicationNodes(Arrays.asList(replicationHostIds));
    }

    private List<NetNode> getReplicationNodes(List<String> replicationHostIds) {
        int size = replicationHostIds.size();
        List<NetNode> replicationNodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            replicationNodes.add(db.getNode(replicationHostIds.get(i)));
        }
        return replicationNodes;
    }

    @Override
    public void replicate(Object dbObject, String[] newReplicationNodes, RunMode runMode) {
        replicateRootPages(dbObject, null, newReplicationNodes, runMode);
    }

    @Override
    public void sharding(Object dbObject, String[] oldNodes, String[] newNodes, RunMode runMode) {
        replicateRootPages(dbObject, oldNodes, newNodes, runMode);
    }

    private void replicateRootPages(Object dbObject, String[] oldNodes, String[] targetNodes, RunMode runMode) {
        List<NetNode> replicationNodes = getReplicationNodes(targetNodes);
        // 用最高权限的用户移动页面，因为目标节点上可能还没有对应的数据库
        IDatabase db = (IDatabase) dbObject;
        Session session = db.createInternalSession(true);
        ReplicationSession rs = db.createReplicationSession(session, replicationNodes);
        int id = db.getId();
        String sysMapName = "t_" + id + "_0";
        try (DataBuffer p = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
            HashMap<String, StorageMap<?, ?>> maps = new HashMap<>(this.maps);
            Collection<StorageMap<?, ?>> values = maps.values();
            p.putInt(values.size());
            // SYS表放在前面，并且总是使用CLIENT_SERVER模式
            StorageMap<?, ?> sysMap = maps.remove(sysMapName);
            replicateRootPage(db, sysMap, p, oldNodes, RunMode.CLIENT_SERVER);
            for (StorageMap<?, ?> map : values) {
                replicateRootPage(db, map, p, oldNodes, runMode);
            }
            ByteBuffer pageBuffer = p.getAndFlipBuffer();
            c.replicateRootPages(db.getShortName(), pageBuffer);
            db.notifyRunModeChanged();
        }
    }

    private void replicateRootPage(IDatabase db, StorageMap<?, ?> map, DataBuffer p, String[] oldNodes,
            RunMode runMode) {
        map = map.getRawMap();
        if (map instanceof DistributedBTreeMap) {
            String mapName = map.getName();
            ValueString.type.write(p, mapName);

            DistributedBTreeMap<?, ?> btreeMap = (DistributedBTreeMap<?, ?>) map;
            btreeMap.setOldNodes(oldNodes);
            btreeMap.setDatabase(db);
            btreeMap.setRunMode(runMode);
            btreeMap.replicateRootPage(p);
        }
    }

    @Override
    public void scaleIn(Object dbObject, RunMode oldRunMode, RunMode newRunMode, String[] oldNodes, String[] newNodes) {
        IDatabase db = (IDatabase) dbObject;
        for (StorageMap<?, ?> map : maps.values()) {
            map = map.getRawMap();
            if (map instanceof BTreeMap) {
                DistributedBTreeMap<?, ?> btreeMap = (DistributedBTreeMap<?, ?>) map;
                btreeMap.setOldNodes(oldNodes);
                btreeMap.setDatabase(db);
                btreeMap.setRunMode(newRunMode);
                if (oldNodes == null) {
                    btreeMap.replicateAllRemotePages();
                } else {
                    btreeMap.moveAllLocalLeafPages(oldNodes, newNodes);
                }
            }
        }
        db.notifyRunModeChanged();
    }
}
