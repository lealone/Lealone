/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.statement;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class StatementUpdateAck implements AckPacket {

    public final int updateCount;

    public StatementUpdateAck(int updateCount) {
        this.updateCount = updateCount;
    }

    public StatementUpdateAck(NetInputStream in, int version) throws IOException {
        updateCount = in.readInt();
    }

    @Override
    public PacketType getType() {
        return PacketType.STATEMENT_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(updateCount);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StatementUpdateAck> {
        @Override
        public StatementUpdateAck decode(NetInputStream in, int version) throws IOException {
            return new StatementUpdateAck(in, version);
        }
    }
}
