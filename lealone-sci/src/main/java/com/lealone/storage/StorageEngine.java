/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage;

import com.lealone.db.Constants;
import com.lealone.db.DataHandler;
import com.lealone.db.plugin.PluggableEngine;
import com.lealone.db.plugin.PluginManager;
import com.lealone.storage.lob.LobStorage;

public interface StorageEngine extends PluggableEngine {

    StorageBuilder getStorageBuilder();

    LobStorage getLobStorage(DataHandler dataHandler, Storage storage);

    public static StorageEngine getDefaultStorageEngine() {
        return PluginManager.getPlugin(StorageEngine.class, Constants.DEFAULT_STORAGE_ENGINE_NAME);
    }
}
