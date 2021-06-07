/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.NoAckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class DTransactionAddSavepoint implements NoAckPacket {

    public final String name;

    public DTransactionAddSavepoint(String name) {
        this.name = name;
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(name);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionAddSavepoint> {
        @Override
        public DTransactionAddSavepoint decode(NetInputStream in, int version) throws IOException {
            String name = in.readString();
            return new DTransactionAddSavepoint(name);
        }
    }
}
