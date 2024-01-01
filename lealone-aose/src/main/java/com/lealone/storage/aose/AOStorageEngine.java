/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose;

import com.lealone.db.DataHandler;
import com.lealone.storage.Storage;
import com.lealone.storage.StorageBuilder;
import com.lealone.storage.StorageEngineBase;
import com.lealone.storage.aose.lob.LobStreamStorage;
import com.lealone.storage.lob.LobStorage;

public class AOStorageEngine extends StorageEngineBase {

    public static final String NAME = "AOSE";

    public AOStorageEngine() {
        super(NAME);
    }

    @Override
    public StorageBuilder getStorageBuilder() {
        return new AOStorageBuilder(config);
    }

    @Override
    public LobStorage getLobStorage(DataHandler dataHandler, Storage storage) {
        return new LobStreamStorage(dataHandler, storage);
    }
}
