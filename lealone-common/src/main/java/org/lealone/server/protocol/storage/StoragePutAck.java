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

public class StoragePutAck extends StorageOperationAck {

    public final ByteBuffer result;

    public StoragePutAck(ByteBuffer result, String localTransactionNames) {
        super(localTransactionNames);
        this.result = result;
    }

    public StoragePutAck(NetInputStream in, int version) throws IOException {
        super(in, version);
        result = in.readByteBuffer();
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_PUT_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeByteBuffer(result);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StoragePutAck> {
        @Override
        public StoragePutAck decode(NetInputStream in, int version) throws IOException {
            return new StoragePutAck(in, version);
        }
    }
}
