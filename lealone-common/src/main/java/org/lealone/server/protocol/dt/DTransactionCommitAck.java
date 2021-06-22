/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class DTransactionCommitAck implements AckPacket {

    public DTransactionCommitAck() {
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_COMMIT_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionCommitAck> {
        @Override
        public DTransactionCommitAck decode(NetInputStream in, int version) throws IOException {
            return new DTransactionCommitAck();
        }
    }
}
