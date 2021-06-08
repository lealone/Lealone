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

public class StoragePut extends StorageWrite {

    public final ByteBuffer key;
    public final ByteBuffer value;
    public final boolean raw;
    public final boolean addIfAbsent;

    public StoragePut(String mapName, ByteBuffer key, ByteBuffer value, boolean isDistributedTransaction,
            String replicationName, boolean raw, boolean addIfAbsent) {
        super(mapName, isDistributedTransaction, replicationName);
        this.key = key;
        this.value = value;
        this.raw = raw;
        this.addIfAbsent = addIfAbsent;
    }

    public StoragePut(NetInputStream in, int version) throws IOException {
        super(in, version);
        key = in.readByteBuffer();
        value = in.readByteBuffer();
        raw = in.readBoolean();
        addIfAbsent = in.readBoolean();
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_PUT;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STORAGE_PUT_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeByteBuffer(key).writeByteBuffer(value).writeBoolean(raw).writeBoolean(addIfAbsent);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StoragePut> {
        @Override
        public StoragePut decode(NetInputStream in, int version) throws IOException {
            return new StoragePut(in, version);
        }
    }
}
