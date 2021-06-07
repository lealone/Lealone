/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.replication;

import java.io.IOException;

import org.lealone.db.value.Value;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.ps.PreparedStatementUpdate;

public class ReplicationPreparedUpdate extends PreparedStatementUpdate {

    public final String replicationName;

    public ReplicationPreparedUpdate(int commandId, Value[] parameters, String replicationName) {
        super(commandId, parameters);
        this.replicationName = replicationName;
    }

    public ReplicationPreparedUpdate(NetInputStream in, int version) throws IOException {
        super(in, version);
        replicationName = in.readString();
    }

    @Override
    public PacketType getType() {
        return PacketType.REPLICATION_PREPARED_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.REPLICATION_PREPARED_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeString(replicationName);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReplicationPreparedUpdate> {
        @Override
        public ReplicationPreparedUpdate decode(NetInputStream in, int version) throws IOException {
            return new ReplicationPreparedUpdate(in, version);
        }
    }
}
