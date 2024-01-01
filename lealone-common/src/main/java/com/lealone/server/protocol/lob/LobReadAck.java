/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.lob;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

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
