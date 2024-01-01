/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.ps;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class PreparedStatementPrepareAck implements AckPacket {

    public final boolean isQuery;

    public PreparedStatementPrepareAck(boolean isQuery) {
        this.isQuery = isQuery;
    }

    @Override
    public PacketType getType() {
        return PacketType.PREPARED_STATEMENT_PREPARE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeBoolean(isQuery);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PreparedStatementPrepareAck> {
        @Override
        public PreparedStatementPrepareAck decode(NetInputStream in, int version) throws IOException {
            return new PreparedStatementPrepareAck(in.readBoolean());
        }
    }
}
