/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.replication;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.storage.replication.ReplicationConflictType;

public class ReplicationPreparedUpdateAck extends ReplicationUpdateAck {

    public ReplicationPreparedUpdateAck(int updateCount, long first, String uncommittedReplicationName,
            ReplicationConflictType replicationConflictType, int ackVersion, boolean isIfDDL, boolean isFinalResult) {
        super(updateCount, first, uncommittedReplicationName, replicationConflictType, ackVersion, isIfDDL,
                isFinalResult);
    }

    public ReplicationPreparedUpdateAck(NetInputStream in, int version) throws IOException {
        super(in, version);
    }

    @Override
    public PacketType getType() {
        return PacketType.REPLICATION_PREPARED_UPDATE_ACK;
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReplicationPreparedUpdateAck> {
        @Override
        public ReplicationPreparedUpdateAck decode(NetInputStream in, int version) throws IOException {
            return new ReplicationPreparedUpdateAck(in, version);
        }
    }
}
