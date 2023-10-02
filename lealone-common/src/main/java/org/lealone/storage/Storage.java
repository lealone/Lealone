/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.util.Map;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;

public interface Storage {

    default <K, V> StorageMap<K, V> openMap(String name, Map<String, String> parameters) {
        return openMap(name, new ObjectDataType(), new ObjectDataType(), parameters);
    }

    <K, V> StorageMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, String> parameters);

    void closeMap(String name);

    boolean hasMap(String name);

    StorageMap<?, ?> getMap(String name);

    Set<String> getMapNames();

    String nextTemporaryMapName();

    String getStorageName();

    String getStoragePath();

    boolean isInMemory();

    long getDiskSpaceUsed();

    long getMemorySpaceUsed();

    void save();

    void drop();

    default void backupTo(String fileName) {
        backupTo(fileName, null);
    }

    void backupTo(String fileName, Long lastDate);

    void backupTo(String baseDir, ZipOutputStream out, Long lastDate);

    void close();

    void closeImmediately();

    boolean isClosed();

    void registerEventListener(StorageEventListener listener);

    void unregisterEventListener(StorageEventListener listener);
}
