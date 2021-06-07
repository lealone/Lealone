/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.replication;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class ReplicationCheckConflictAck implements AckPacket {

    public final String replicationName;

    public ReplicationCheckConflictAck(String replicationName) {
        this.replicationName = replicationName;
    }

    @Override
    public PacketType getType() {
        return PacketType.REPLICATION_CHECK_CONFLICT_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(replicationName);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReplicationCheckConflictAck> {
        @Override
        public ReplicationCheckConflictAck decode(NetInputStream in, int version) throws IOException {
            String replicationName = in.readString();
            return new ReplicationCheckConflictAck(replicationName);
        }
    }
}
