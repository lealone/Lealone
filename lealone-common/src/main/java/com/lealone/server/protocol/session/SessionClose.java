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

public class SessionClose implements NoAckPacket {

    public SessionClose() {
    }

    @Override
    public PacketType getType() {
        return PacketType.SESSION_CLOSE;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<SessionClose> {
        @Override
        public SessionClose decode(NetInputStream in, int version) throws IOException {
            return new SessionClose();
        }
    }
}
