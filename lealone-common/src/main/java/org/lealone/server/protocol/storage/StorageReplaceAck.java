/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.storage;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class StorageReplaceAck extends StorageOperationAck {

    public final boolean result;

    public StorageReplaceAck(boolean result, String localTransactionNames) {
        super(localTransactionNames);
        this.result = result;
    }

    public StorageReplaceAck(NetInputStream in, int version) throws IOException {
        super(in, version);
        result = in.readBoolean();
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_REPLACE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeBoolean(result);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageReplaceAck> {
        @Override
        public StorageReplaceAck decode(NetInputStream in, int version) throws IOException {
            return new StorageReplaceAck(in, version);
        }
    }
}
