/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.replication;

import java.util.List;

import org.lealone.db.Command;

//与单个副本相关的命令
public interface ReplicaCommand extends Command {

    default void handleReplicaConflict(List<String> retryReplicationNames) {
    }

    default void removeAsyncCallback(int packetId) {
    }
}
