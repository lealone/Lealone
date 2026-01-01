/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.ps;

import java.io.IOException;

import com.lealone.db.result.Result;
import com.lealone.db.result.ResultColumn;
import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class PreparedStatementGetMetaDataAck implements AckPacket {

    public final Result result;
    public final int columnCount;
    public final NetInputStream in;

    public PreparedStatementGetMetaDataAck(Result result) {
        this.result = result;
        columnCount = result.getVisibleColumnCount();
        in = null;
    }

    public PreparedStatementGetMetaDataAck(NetInputStream in, int columnCount) {
        result = null;
        this.columnCount = columnCount;
        this.in = in;
    }

    @Override
    public PacketType getType() {
        return PacketType.PREPARED_STATEMENT_GET_META_DATA_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(columnCount);
        for (int i = 0; i < columnCount; i++) {
            ResultColumn.write(out, result, i);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PreparedStatementGetMetaDataAck> {
        @Override
        public PreparedStatementGetMetaDataAck decode(NetInputStream in, int version)
                throws IOException {
            int columnCount = in.readInt();
            return new PreparedStatementGetMetaDataAck(in, columnCount);
        }
    }
}
