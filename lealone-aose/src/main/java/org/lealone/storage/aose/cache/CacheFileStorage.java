/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.cache;

import java.nio.channels.FileChannel;

import org.lealone.storage.fs.FileStorage;

public class CacheFileStorage extends FileStorage {
    @Override
    protected FileChannel wrap(FileChannel file) {
        return FilePathCache.wrap(file);
    }
}
