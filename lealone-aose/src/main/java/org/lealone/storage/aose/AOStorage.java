/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose;

import java.io.InputStream;
import java.util.Map;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DbSetting;
import org.lealone.storage.StorageBase;
import org.lealone.storage.StorageEventListener;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageSetting;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.type.StorageDataType;

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
        return openBTreeMap(mapName).getInputStream(file);
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
                    maps.put(name, map);
                    for (StorageEventListener listener : listeners.values())
                        listener.afterStorageMapOpen(map);
                }
            }
        }
        return (BTreeMap<K, V>) map;
    }
}
