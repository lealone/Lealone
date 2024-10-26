/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DataHandler;
import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginBase;
import com.lealone.storage.lob.LobStorage;

public abstract class StorageEngineBase extends PluginBase implements StorageEngine {

    public StorageEngineBase(String name) {
        super(name);
    }

    @Override
    public LobStorage getLobStorage(DataHandler dataHandler, Storage storage) {
        throw DbException.getUnsupportedException("getLobStorage");
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return StorageEngine.class;
    }
}
