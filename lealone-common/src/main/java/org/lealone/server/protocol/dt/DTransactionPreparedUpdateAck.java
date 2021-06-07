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

public class DTransactionPreparedUpdateAck implements AckPacket {

    public final int updateCount;
    public final String localTransactionNames;

    public DTransactionPreparedUpdateAck(int updateCount, String localTransactionNames) {
        this.updateCount = updateCount;
        this.localTransactionNames = localTransactionNames;
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(updateCount).writeString(localTransactionNames);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionPreparedUpdateAck> {
        @Override
        public DTransactionPreparedUpdateAck decode(NetInputStream in, int version) throws IOException {
            return new DTransactionPreparedUpdateAck(in.readInt(), in.readString());
        }
    }
}
