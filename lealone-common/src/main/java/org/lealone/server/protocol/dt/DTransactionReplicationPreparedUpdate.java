/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.db.value.Value;
import org.lealone.net.NetInputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.replication.ReplicationPreparedUpdate;

public class DTransactionReplicationPreparedUpdate extends ReplicationPreparedUpdate {

    public DTransactionReplicationPreparedUpdate(int commandId, Value[] parameters, String replicationName) {
        super(commandId, parameters, replicationName);
    }

    public DTransactionReplicationPreparedUpdate(NetInputStream in, int version) throws IOException {
        super(in, version);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_REPLICATION_PREPARED_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_REPLICATION_PREPARED_UPDATE_ACK;
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionReplicationPreparedUpdate> {
        @Override
        public DTransactionReplicationPreparedUpdate decode(NetInputStream in, int version) throws IOException {
            return new DTransactionReplicationPreparedUpdate(in, version);
        }
    }
}
