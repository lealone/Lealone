/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.util.Map;

import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.type.StorageDataType;

public class BTreeMapBuilder<K, V> {

    protected String name;
    protected StorageDataType keyType;
    protected StorageDataType valueType;
    protected Map<String, Object> config;
    protected AOStorage aoStorage;

    public BTreeMapBuilder<K, V> name(String name) {
        this.name = name;
        return this;
    }

    public BTreeMapBuilder<K, V> keyType(StorageDataType keyType) {
        this.keyType = keyType;
        return this;
    }

    public BTreeMapBuilder<K, V> valueType(StorageDataType valueType) {
        this.valueType = valueType;
        return this;
    }

    public BTreeMapBuilder<K, V> config(Map<String, Object> config) {
        this.config = config;
        return this;
    }

    public BTreeMapBuilder<K, V> aoStorage(AOStorage aoStorage) {
        this.aoStorage = aoStorage;
        return this;
    }

    public BTreeMap<K, V> openMap() {
        return new BTreeMap<K, V>(name, keyType, valueType, config, aoStorage);
    }
}
