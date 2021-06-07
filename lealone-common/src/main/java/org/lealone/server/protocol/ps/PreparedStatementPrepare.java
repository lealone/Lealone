/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.ps;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class PreparedStatementPrepare implements Packet {

    public final int commandId;
    public final String sql;

    public PreparedStatementPrepare(int commandId, String sql) {
        this.commandId = commandId;
        this.sql = sql;
    }

    @Override
    public PacketType getType() {
        return PacketType.PREPARED_STATEMENT_PREPARE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.PREPARED_STATEMENT_PREPARE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(commandId).writeString(sql);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PreparedStatementPrepare> {
        @Override
        public PreparedStatementPrepare decode(NetInputStream in, int version) throws IOException {
            int commandId = in.readInt();
            String sql = in.readString();
            return new PreparedStatementPrepare(commandId, sql);
        }
    }
}
