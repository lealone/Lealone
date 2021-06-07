/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.storage;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class StorageAppendAck implements AckPacket {

    public final long result;
    public final String localTransactionNames;

    public StorageAppendAck(long result, String localTransactionNames) {
        this.result = result;
        this.localTransactionNames = localTransactionNames;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_APPEND_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeLong(result).writeString(localTransactionNames);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageAppendAck> {
        @Override
        public StorageAppendAck decode(NetInputStream in, int version) throws IOException {
            return new StorageAppendAck(in.readLong(), in.readString());
        }
    }
}
