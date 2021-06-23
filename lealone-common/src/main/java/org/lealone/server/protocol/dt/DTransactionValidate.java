/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class DTransactionValidate implements Packet {

    public final String globalTransactionName;

    public DTransactionValidate(String globalTransactionName) {
        this.globalTransactionName = globalTransactionName;
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_VALIDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_VALIDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(globalTransactionName);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionValidate> {
        @Override
        public DTransactionValidate decode(NetInputStream in, int version) throws IOException {
            return new DTransactionValidate(in.readString());
        }
    }
}
