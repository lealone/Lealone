/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.replication;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.NoAckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class ReplicationHandleConflict implements NoAckPacket {

    public final String mapName;
    public final ByteBuffer key;
    public final String replicationName;

    public ReplicationHandleConflict(String mapName, ByteBuffer key, String replicationName) {
        this.mapName = mapName;
        this.key = key;
        this.replicationName = replicationName;
    }

    @Override
    public PacketType getType() {
        return PacketType.REPLICATION_HANDLE_CONFLICT;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(mapName).writeByteBuffer(key).writeString(replicationName);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReplicationHandleConflict> {
        @Override
        public ReplicationHandleConflict decode(NetInputStream in, int version) throws IOException {
            String mapName = in.readString();
            ByteBuffer key = in.readByteBuffer();
            String replicationName = in.readString();
            return new ReplicationHandleConflict(mapName, key, replicationName);
        }
    }
}
