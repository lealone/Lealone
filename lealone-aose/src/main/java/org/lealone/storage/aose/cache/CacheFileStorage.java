/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.cache;

import java.nio.channels.FileChannel;
import java.util.Map;

import org.lealone.storage.fs.FileStorage;

public class CacheFileStorage extends FileStorage {

    protected CacheFileStorage(String fileName, Map<String, ?> config) {
        super(fileName, config);
    }

    @Override
    protected FileChannel wrap(FileChannel file) {
        return FilePathCache.wrap(file);
    }

    public static FileStorage open(String fileName, Map<String, ?> config) {
        return new CacheFileStorage(fileName, config);
    }
}
