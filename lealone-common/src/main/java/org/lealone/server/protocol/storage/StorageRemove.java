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

public class StorageRemove extends StorageWrite {

    public final ByteBuffer key;

    public StorageRemove(String mapName, ByteBuffer key, boolean isDistributedTransaction, String replicationName) {
        super(mapName, isDistributedTransaction, replicationName);
        this.key = key;
    }

    public StorageRemove(NetInputStream in, int version) throws IOException {
        super(in, version);
        key = in.readByteBuffer();
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_REMOVE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STORAGE_REMOVE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeByteBuffer(key);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageRemove> {
        @Override
        public StorageRemove decode(NetInputStream in, int version) throws IOException {
            return new StorageRemove(in, version);
        }
    }
}
