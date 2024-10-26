/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage;

public interface StorageEventListener {

    void beforeClose(Storage storage);

    // void beforeDrop(Storage storage);

    void afterStorageMapOpen(StorageMap<?, ?> map);

}
