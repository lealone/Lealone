/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.db.value.Value;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.replication.ReplicationPreparedUpdate;

public class DTransactionReplicationPreparedUpdate extends ReplicationPreparedUpdate {

    public final DTransactionParameters parameters;

    public DTransactionReplicationPreparedUpdate(int commandId, Value[] parameters, String replicationName,
            DTransactionParameters dtParameters) {
        super(commandId, parameters, replicationName);
        this.parameters = dtParameters;
    }

    public DTransactionReplicationPreparedUpdate(NetInputStream in, int version) throws IOException {
        super(in, version);
        parameters = new DTransactionParameters(in, version);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_REPLICATION_PREPARED_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_REPLICATION_PREPARED_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        parameters.encode(out, version);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionReplicationPreparedUpdate> {
        @Override
        public DTransactionReplicationPreparedUpdate decode(NetInputStream in, int version) throws IOException {
            return new DTransactionReplicationPreparedUpdate(in, version);
        }
    }
}
