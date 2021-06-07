/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.lob;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class LobReadAck implements AckPacket {

    public final byte[] buff;

    public LobReadAck(byte[] buff) {
        this.buff = buff;
    }

    @Override
    public PacketType getType() {
        return PacketType.LOB_READ_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeBytes(buff);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<LobReadAck> {
        @Override
        public LobReadAck decode(NetInputStream in, int version) throws IOException {
            return new LobReadAck(in.readBytes());
        }
    }
}
