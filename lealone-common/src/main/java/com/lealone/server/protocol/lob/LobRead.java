/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.lob;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class LobRead implements Packet {

    public final long lobId;
    public final byte[] hmac;
    public final long offset;
    public final int length;

    public LobRead(long lobId, byte[] hmac, long offset, int length) {
        this.lobId = lobId;
        this.hmac = hmac;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public PacketType getType() {
        return PacketType.LOB_READ;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.LOB_READ_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeLong(lobId).writeBytes(hmac).writeLong(offset).writeInt(length);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<LobRead> {
        @Override
        public LobRead decode(NetInputStream in, int version) throws IOException {
            return new LobRead(in.readLong(), in.readBytes(), in.readLong(), in.readInt());
        }
    }
}
