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
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class StorageReplace implements Packet {

    public final String mapName;
    public final ByteBuffer key;
    public final ByteBuffer oldValue;
    public final ByteBuffer newValue;
    public final boolean isDistributedTransaction;
    public final String replicationName;

    public StorageReplace(String mapName, ByteBuffer key, ByteBuffer oldValue, ByteBuffer newValue,
            boolean isDistributedTransaction, String replicationName) {
        this.mapName = mapName;
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.isDistributedTransaction = isDistributedTransaction;
        this.replicationName = replicationName;
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
        out.writeString(mapName).writeByteBuffer(key).writeByteBuffer(oldValue).writeByteBuffer(newValue)
                .writeBoolean(isDistributedTransaction).writeString(replicationName);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageReplace> {
        @Override
        public StorageReplace decode(NetInputStream in, int version) throws IOException {
            return new StorageReplace(in.readString(), in.readByteBuffer(), in.readByteBuffer(), in.readByteBuffer(),
                    in.readBoolean(), in.readString());
        }
    }
}
