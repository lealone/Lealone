/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.ps;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

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
