/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.session;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.NoAckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class SessionSetAutoCommit implements NoAckPacket {

    public final boolean autoCommit;

    public SessionSetAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public PacketType getType() {
        return PacketType.SESSION_SET_AUTO_COMMIT;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeBoolean(autoCommit);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<SessionSetAutoCommit> {
        @Override
        public SessionSetAutoCommit decode(NetInputStream in, int version) throws IOException {
            return new SessionSetAutoCommit(in.readBoolean());
        }
    }
}
