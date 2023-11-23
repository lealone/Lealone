/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose;

import java.util.HashMap;
import java.util.Map;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.scheduler.EmbeddedScheduler;
import org.lealone.db.scheduler.SchedulerFactory;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageSetting;

public class AOStorageBuilder extends StorageBuilder {

    private static final HashMap<String, AOStorage> cache = new HashMap<>();

    public AOStorageBuilder() {
        this(null);
    }

    public AOStorageBuilder(Map<String, String> defaultConfig) {
        if (defaultConfig != null)
            config.putAll(defaultConfig);
    }

    @Override
    public AOStorage openStorage() {
        String storagePath = (String) config.get(StorageSetting.STORAGE_PATH.name());
        AOStorage storage = cache.get(storagePath);
        if (storage == null) {
            synchronized (cache) {
                storage = cache.get(storagePath);
                if (storage == null) {
                    storage = new AOStorage(config);
                    SchedulerFactory sf = getSchedulerFactory();
                    storage.setSchedulerFactory(sf);
                    cache.put(storagePath, storage);
                }
            }
        }
        return storage;
    }

    private SchedulerFactory getSchedulerFactory() {
        SchedulerFactory sf = (SchedulerFactory) config.get(StorageSetting.SCHEDULER_FACTORY.name());
        if (sf == null) {
            sf = SchedulerFactory.getDefaultSchedulerFactory();
            if (sf == null) {
                CaseInsensitiveMap<String> config = new CaseInsensitiveMap<>(this.config.size());
                for (Map.Entry<String, Object> e : this.config.entrySet()) {
                    config.put(e.getKey(), e.getValue().toString());
                }
                sf = SchedulerFactory.initDefaultSchedulerFactory(EmbeddedScheduler.class.getName(),
                        config);
            }
        }
        return sf;
    }
}
