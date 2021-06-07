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

public class StorageRemoveAck implements AckPacket {

    public final ByteBuffer result;
    public final String localTransactionNames;

    public StorageRemoveAck(ByteBuffer result, String localTransactionNames) {
        this.result = result;
        this.localTransactionNames = localTransactionNames;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_REMOVE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeByteBuffer(result).writeString(localTransactionNames);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageRemoveAck> {
        @Override
        public StorageRemoveAck decode(NetInputStream in, int version) throws IOException {
            return new StorageRemoveAck(in.readByteBuffer(), in.readString());
        }
    }
}
