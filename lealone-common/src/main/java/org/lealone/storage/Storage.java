/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
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

    String getStoragePath();

    boolean isInMemory();

    long getDiskSpaceUsed();

    long getMemorySpaceUsed();

    void save();

    void drop();

    void backupTo(String fileName);

    void close();

    void closeImmediately();

    boolean isClosed();

    void registerEventListener(StorageEventListener listener);

    void unregisterEventListener(StorageEventListener listener);

    default void replicateFrom(ByteBuffer data) {
        throw DbException.getUnsupportedException("replicateFrom");
    }

    default void scaleOut(IDatabase db, RunMode oldRunMode, RunMode newRunMode, String[] oldNodes, String[] newNodes) {
        throw DbException.getUnsupportedException("scaleOut");
    }

    default void scaleIn(IDatabase db, RunMode oldRunMode, RunMode newRunMode, String[] oldNodes, String[] newNodes) {
        throw DbException.getUnsupportedException("scaleIn");
    }
}
