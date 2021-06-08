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
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class StorageAppend extends StorageWrite {

    public final ByteBuffer value;

    public StorageAppend(String mapName, ByteBuffer value, boolean isDistributedTransaction, String replicationName) {
        super(mapName, isDistributedTransaction, replicationName);
        this.value = value;
    }

    public StorageAppend(NetInputStream in, int version) throws IOException {
        super(in, version);
        value = in.readByteBuffer();
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_APPEND;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STORAGE_APPEND_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeByteBuffer(value);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageAppend> {
        @Override
        public StorageAppend decode(NetInputStream in, int version) throws IOException {
            return new StorageAppend(in, version);
        }
    }
}
