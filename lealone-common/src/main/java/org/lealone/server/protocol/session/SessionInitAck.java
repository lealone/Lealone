/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.session;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class SessionInitAck implements AckPacket {

    public final int clientVersion;
    public final boolean autoCommit;

    public SessionInitAck(int clientVersion, boolean autoCommit) {
        this.clientVersion = clientVersion;
        this.autoCommit = autoCommit;
    }

    @Override
    public PacketType getType() {
        return PacketType.SESSION_INIT_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(clientVersion);
        out.writeBoolean(autoCommit);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<SessionInitAck> {
        @Override
        public SessionInitAck decode(NetInputStream in, int version) throws IOException {
            int clientVersion = in.readInt();
            boolean autoCommit = in.readBoolean();
            return new SessionInitAck(clientVersion, autoCommit);
        }
    }
}
