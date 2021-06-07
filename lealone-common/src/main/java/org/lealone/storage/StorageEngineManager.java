/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import org.lealone.db.PluggableEngineManager;

public class StorageEngineManager extends PluggableEngineManager<StorageEngine> {

    private static final StorageEngineManager instance = new StorageEngineManager();

    public static StorageEngineManager getInstance() {
        return instance;
    }

    private StorageEngineManager() {
        super(StorageEngine.class);
    }

    public static StorageEngine getStorageEngine(String name) {
        return instance.getEngine(name);
    }

}
