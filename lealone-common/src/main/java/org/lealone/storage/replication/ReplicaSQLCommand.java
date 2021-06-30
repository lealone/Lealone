/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.replication;

import org.lealone.db.async.Future;
import org.lealone.server.protocol.dt.DTransactionParameters;
import org.lealone.server.protocol.replication.ReplicationUpdateAck;
import org.lealone.sql.DistributedSQLCommand;

public interface ReplicaSQLCommand extends ReplicaCommand, DistributedSQLCommand {

    Future<ReplicationUpdateAck> executeReplicaUpdate(String replicationName, DTransactionParameters parameters);

}
