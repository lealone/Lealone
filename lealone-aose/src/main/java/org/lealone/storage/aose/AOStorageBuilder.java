/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose;

import java.util.HashMap;
import java.util.Map;

import org.lealone.storage.StorageBuilder;
import org.lealone.storage.page.PageOperationHandlerFactory;

public class AOStorageBuilder extends StorageBuilder {

    private static final HashMap<String, AOStorage> cache = new HashMap<>();

    public AOStorageBuilder() {
        this(null, null);
    }

    public AOStorageBuilder(Map<String, String> defaultConfig) {
        this(defaultConfig, null);
    }

    public AOStorageBuilder(Map<String, String> defaultConfig, PageOperationHandlerFactory pohFactory) {
        if (pohFactory == null)
            pohFactory = PageOperationHandlerFactory.create(defaultConfig);
        if (defaultConfig != null)
            config.putAll(defaultConfig);
        config.put("pohFactory", pohFactory);
    }

    @Override
    public AOStorage openStorage() {
        String storagePath = (String) config.get("storagePath");
        AOStorage storage = cache.get(storagePath);
        if (storage == null) {
            synchronized (cache) {
                storage = cache.get(storagePath);
                if (storage == null) {
                    storage = new AOStorage(config);
                    cache.put(storagePath, storage);
                }
            }
        }
        return storage;
    }
}
