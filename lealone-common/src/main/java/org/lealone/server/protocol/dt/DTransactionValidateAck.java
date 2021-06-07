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

public class DTransactionValidateAck implements AckPacket {

    public final boolean isValid;

    public DTransactionValidateAck(boolean isValid) {
        this.isValid = isValid;
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_VALIDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeBoolean(isValid);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionValidateAck> {
        @Override
        public DTransactionValidateAck decode(NetInputStream in, int version) throws IOException {
            return new DTransactionValidateAck(in.readBoolean());
        }
    }
}
