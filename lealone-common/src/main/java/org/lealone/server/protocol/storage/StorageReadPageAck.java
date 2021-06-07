/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class StorageReadPageAck implements AckPacket {

    public final ByteBuffer page;

    public StorageReadPageAck(ByteBuffer page) {
        this.page = page;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_READ_PAGE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeByteBuffer(page);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageReadPageAck> {
        @Override
        public StorageReadPageAck decode(NetInputStream in, int version) throws IOException {
            return new StorageReadPageAck(in.readByteBuffer());
        }
    }
}
