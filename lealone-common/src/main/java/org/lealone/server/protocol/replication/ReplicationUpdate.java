/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.replication;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.statement.StatementUpdate;

public class ReplicationUpdate extends StatementUpdate {

    public final String replicationName;

    public ReplicationUpdate(String sql, String replicationName) {
        super(sql);
        this.replicationName = replicationName;
    }

    public ReplicationUpdate(NetInputStream in, int version) throws IOException {
        super(in, version);
        replicationName = in.readString();
    }

    @Override
    public PacketType getType() {
        return PacketType.REPLICATION_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.REPLICATION_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeString(replicationName);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReplicationUpdate> {
        @Override
        public ReplicationUpdate decode(NetInputStream in, int version) throws IOException {
            return new ReplicationUpdate(in, version);
        }
    }
}
