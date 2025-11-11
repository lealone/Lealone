/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose;

import java.io.InputStream;
import java.util.Map;

import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.DataUtils;
import com.lealone.db.DbSetting;
import com.lealone.storage.StorageBase;
import com.lealone.storage.StorageEventListener;
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.fs.FilePath;
import com.lealone.storage.fs.FileUtils;
import com.lealone.storage.type.StorageDataType;

/**
 * Adaptive optimization storage
 */
public class AOStorage extends StorageBase {

    public static final String SUFFIX_AO_FILE = ".db";
    public static final int SUFFIX_AO_FILE_LENGTH = SUFFIX_AO_FILE.length();

    AOStorage(Map<String, Object> config) {
        super(config);
        if (config.containsKey(StorageSetting.IN_MEMORY.name()))
            return;
        String storagePath = getStoragePath();
        DataUtils.checkNotNull(storagePath, "storage path");
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
    protected InputStream getInputStream(String mapName, FilePath file) {
        return openBTreeMap(mapName).getBTreeStorage().getChunkManager().getChunkInputStream(file);
    }

    @Override
    public String getStorageName() {
        return AOStorageEngine.NAME;
    }

    public boolean isReadOnly() {
        return config.containsKey(DbSetting.READ_ONLY.name());
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, StorageDataType keyType,
            StorageDataType valueType, Map<String, String> parameters) {
        String mapType = parameters == null ? null : parameters.get(StorageSetting.MAP_TYPE.name());
        return openMap(name, mapType, keyType, valueType, parameters);
    }

    public <K, V> StorageMap<K, V> openMap(String name, String mapType, StorageDataType keyType,
            StorageDataType valueType, Map<String, String> parameters) {
        if (mapType == null || mapType.equalsIgnoreCase("BTreeMap")) {
            return openBTreeMap(name, keyType, valueType, parameters);
        } else {
            throw DataUtils.newIllegalArgumentException("Unknow map type: {0}", mapType);
        }
    }

    public <K, V> BTreeMap<K, V> openBTreeMap(String name) {
        return openBTreeMap(name, null, null, null);
    }

    @SuppressWarnings("unchecked")
    public <K, V> BTreeMap<K, V> openBTreeMap(String name, StorageDataType keyType,
            StorageDataType valueType, Map<String, String> parameters) {
        StorageMap<?, ?> map = maps.get(name);
        if (map == null) {
            synchronized (this) {
                map = maps.get(name);
                if (map == null) {
                    CaseInsensitiveMap<Object> c = new CaseInsensitiveMap<>(config);
                    if (parameters != null)
                        c.putAll(parameters);
                    map = new BTreeMap<>(name, keyType, valueType, c, this);
                    for (StorageEventListener listener : listeners.values())
                        listener.afterStorageMapOpen(map);
                    // 执行完afterStorageMapOpen后再put，确保其他线程拿到的是一个就绪后的map
                    maps.put(name, map);
                }
            }
        }
        return (BTreeMap<K, V>) map;
    }
}
