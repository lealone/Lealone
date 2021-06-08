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

public class StorageReplace extends StorageWrite {

    public final ByteBuffer key;
    public final ByteBuffer oldValue;
    public final ByteBuffer newValue;

    public StorageReplace(String mapName, ByteBuffer key, ByteBuffer oldValue, ByteBuffer newValue,
            boolean isDistributedTransaction, String replicationName) {
        super(mapName, isDistributedTransaction, replicationName);
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public StorageReplace(NetInputStream in, int version) throws IOException {
        super(in, version);
        key = in.readByteBuffer();
        oldValue = in.readByteBuffer();
        newValue = in.readByteBuffer();
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_REPLACE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STORAGE_REPLACE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeByteBuffer(key).writeByteBuffer(oldValue).writeByteBuffer(newValue);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageReplace> {
        @Override
        public StorageReplace decode(NetInputStream in, int version) throws IOException {
            return new StorageReplace(in, version);
        }
    }
}
