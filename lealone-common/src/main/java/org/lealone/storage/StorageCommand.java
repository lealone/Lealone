/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.nio.ByteBuffer;

import org.lealone.db.Command;
import org.lealone.db.async.Future;

public interface StorageCommand extends Command {

    Future<Object> get(String mapName, ByteBuffer key);

    Future<Object> put(String mapName, ByteBuffer key, ByteBuffer value, boolean raw, boolean addIfAbsent);

    Future<Object> append(String mapName, ByteBuffer value);

    Future<Boolean> replace(String mapName, ByteBuffer key, ByteBuffer oldValue, ByteBuffer newValue);

    Future<Object> remove(String mapName, ByteBuffer key);

    Future<LeafPageMovePlan> prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan);

    void moveLeafPage(String mapName, PageKey pageKey, ByteBuffer page, boolean addPage);

    void replicatePages(String dbName, String storageName, ByteBuffer pages);

    void removeLeafPage(String mapName, PageKey pageKey);

    default Future<ByteBuffer> readRemotePage(String mapName, PageKey pageKey) {
        return null;
    }
}
