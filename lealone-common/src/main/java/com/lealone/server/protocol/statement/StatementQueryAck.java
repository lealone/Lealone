/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.statement;

import java.io.IOException;

import com.lealone.db.result.Result;
import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;
import com.lealone.server.protocol.ps.PreparedStatementGetMetaDataAck;
import com.lealone.server.protocol.result.ResultFetchRowsAck;

public class StatementQueryAck implements AckPacket {

    public final Result result;
    public final int rowCount;
    public final int columnCount;
    public final int fetchSize;
    public final NetInputStream in;

    public StatementQueryAck(Result result, int rowCount, int columnCount, int fetchSize) {
        this.result = result;
        this.rowCount = rowCount;
        this.columnCount = columnCount;
        this.fetchSize = fetchSize;
        in = null;
    }

    public StatementQueryAck(NetInputStream in, int version) throws IOException {
        result = null;
        rowCount = in.readInt();
        columnCount = in.readInt();
        if (rowCount != -2)
            fetchSize = in.readInt();
        else
            fetchSize = 0;
        this.in = in;
    }

    @Override
    public PacketType getType() {
        return PacketType.STATEMENT_QUERY_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(rowCount);
        out.writeInt(columnCount);
        if (rowCount == -2)
            return;
        out.writeInt(fetchSize);
        encodeExt(out, version);
        for (int i = 0; i < columnCount; i++) {
            PreparedStatementGetMetaDataAck.writeColumn(out, result, i);
        }
        ResultFetchRowsAck.writeRow(out, result, fetchSize);
    }

    // ----------------------------------------------------------------
    // 因为查询结果集是lazy解码的，所以子类的字段需要提前编码和解码
    // ----------------------------------------------------------------
    public void encodeExt(NetOutputStream out, int version) throws IOException {
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StatementQueryAck> {
        @Override
        public StatementQueryAck decode(NetInputStream in, int version) throws IOException {
            return new StatementQueryAck(in, version);
        }
    }
}
