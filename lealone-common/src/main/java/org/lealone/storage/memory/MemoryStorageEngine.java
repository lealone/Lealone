/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.memory;

import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngineBase;

public class MemoryStorageEngine extends StorageEngineBase {

    public static final String NAME = "memory";

    public MemoryStorageEngine() {
        super(NAME);
    }

    @Override
    public StorageBuilder getStorageBuilder() {
        return new MemoryStorageBuilder();
    }

    public static class MemoryStorageBuilder extends StorageBuilder {
        @Override
        public MemoryStorage openStorage() {
            return new MemoryStorage();
        }
    }
}
