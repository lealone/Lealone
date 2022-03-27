/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.nio.ByteBuffer;

import org.lealone.db.Command;
import org.lealone.db.async.Future;
import org.lealone.storage.page.LeafPageMovePlan;
import org.lealone.storage.page.PageKey;
import org.lealone.storage.type.StorageDataType;

public interface StorageCommand extends Command {

    Future<Object> get(String mapName, Object key, StorageDataType keyType);

    Future<Object> put(String mapName, Object key, StorageDataType keyType, Object value, StorageDataType valueType,
            boolean raw, boolean addIfAbsent);

    Future<Object> append(String mapName, Object value, StorageDataType valueType);

    Future<Boolean> replace(String mapName, Object key, StorageDataType keyType, Object oldValue, Object newValue,
            StorageDataType valueType);

    Future<Object> remove(String mapName, Object key, StorageDataType keyType);

    Future<LeafPageMovePlan> prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan);

    void moveLeafPage(String mapName, PageKey pageKey, ByteBuffer page, boolean addPage);

    void replicatePages(String dbName, String storageName, ByteBuffer pages);

    void removeLeafPage(String mapName, PageKey pageKey);

    default Future<ByteBuffer> readRemotePage(String mapName, PageKey pageKey) {
        return null;
    }
}
