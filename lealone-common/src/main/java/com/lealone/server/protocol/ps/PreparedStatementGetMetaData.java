/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.ps;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class PreparedStatementGetMetaData implements Packet {

    public final int commandId;

    public PreparedStatementGetMetaData(int commandId) {
        this.commandId = commandId;
    }

    @Override
    public PacketType getType() {
        return PacketType.PREPARED_STATEMENT_GET_META_DATA;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.PREPARED_STATEMENT_GET_META_DATA_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(commandId);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PreparedStatementGetMetaData> {
        @Override
        public PreparedStatementGetMetaData decode(NetInputStream in, int version) throws IOException {
            return new PreparedStatementGetMetaData(in.readInt());
        }
    }
}
