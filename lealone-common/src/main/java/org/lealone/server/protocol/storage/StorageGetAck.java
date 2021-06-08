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

public class StorageGetAck extends StorageOperationAck {

    public final ByteBuffer result;

    public StorageGetAck(ByteBuffer result, String localTransactionNames) {
        super(localTransactionNames);
        this.result = result;
    }

    public StorageGetAck(NetInputStream in, int version) throws IOException {
        super(in, version);
        result = in.readByteBuffer();
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_GET_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeByteBuffer(result);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageGetAck> {
        @Override
        public StorageGetAck decode(NetInputStream in, int version) throws IOException {
            return new StorageGetAck(in, version);
        }
    }
}
