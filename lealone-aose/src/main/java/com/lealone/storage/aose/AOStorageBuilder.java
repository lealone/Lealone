/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose;

import java.util.HashMap;
import java.util.Map;

import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.DataUtils;
import com.lealone.db.scheduler.EmbeddedScheduler;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.storage.StorageBuilder;
import com.lealone.storage.StorageSetting;

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
        if (!config.containsKey(StorageSetting.IN_MEMORY.name()))
            DataUtils.checkNotNull(storagePath, "storage path");
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
        // 嵌入模式下打开新的Storage，如果EmbeddedScheduler没启动则自动启动
        if (!sf.isStarted() && sf.getSchedulerCount() > 0
                && (sf.getScheduler(0) instanceof EmbeddedScheduler)) {
            sf.start();
        }
        return sf;
    }
}
