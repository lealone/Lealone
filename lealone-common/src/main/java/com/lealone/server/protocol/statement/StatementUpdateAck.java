/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.statement;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

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
