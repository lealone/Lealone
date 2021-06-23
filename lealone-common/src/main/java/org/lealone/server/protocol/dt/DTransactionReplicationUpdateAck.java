/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.replication.ReplicationUpdateAck;
import org.lealone.storage.replication.ReplicationConflictType;

public class DTransactionReplicationUpdateAck extends ReplicationUpdateAck {

    public DTransactionReplicationUpdateAck(int updateCount, long first, String uncommittedReplicationName,
            ReplicationConflictType replicationConflictType, int ackVersion, boolean isIfDDL, boolean isFinalResult) {
        super(updateCount, first, uncommittedReplicationName, replicationConflictType, ackVersion, isIfDDL,
                isFinalResult);
    }

    public DTransactionReplicationUpdateAck(NetInputStream in, int version) throws IOException {
        super(in, version);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_REPLICATION_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionReplicationUpdateAck> {
        @Override
        public DTransactionReplicationUpdateAck decode(NetInputStream in, int version) throws IOException {
            return new DTransactionReplicationUpdateAck(in, version);
        }
    }
}
