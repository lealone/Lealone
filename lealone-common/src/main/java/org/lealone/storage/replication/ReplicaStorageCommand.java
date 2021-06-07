/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.replication;

import java.nio.ByteBuffer;

import org.lealone.db.async.Future;
import org.lealone.storage.StorageCommand;

public interface ReplicaStorageCommand extends ReplicaCommand, StorageCommand {

    Future<Object> executeReplicaPut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value,
            boolean raw, boolean addIfAbsent);

    Future<Object> executeReplicaAppend(String replicationName, String mapName, ByteBuffer value);

    Future<Boolean> executeReplicaReplace(String replicationName, String mapName, ByteBuffer key, ByteBuffer oldValue,
            ByteBuffer newValue);

    Future<Object> executeReplicaRemove(String replicationName, String mapName, ByteBuffer key);

}
