/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.ps;

import java.io.IOException;

import org.lealone.db.result.Result;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

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
            writeColumn(out, result, i);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PreparedStatementGetMetaDataAck> {
        @Override
        public PreparedStatementGetMetaDataAck decode(NetInputStream in, int version) throws IOException {
            int columnCount = in.readInt();
            return new PreparedStatementGetMetaDataAck(in, columnCount);
        }
    }

    /**
    * Write a result column to the given output.
    *
    * @param result the result
    * @param i the column index
    */
    public static void writeColumn(NetOutputStream out, Result result, int i) throws IOException {
        out.writeString(result.getAlias(i));
        out.writeString(result.getSchemaName(i));
        out.writeString(result.getTableName(i));
        out.writeString(result.getColumnName(i));
        out.writeInt(result.getColumnType(i));
        out.writeLong(result.getColumnPrecision(i));
        out.writeInt(result.getColumnScale(i));
        out.writeInt(result.getDisplaySize(i));
        out.writeBoolean(result.isAutoIncrement(i));
        out.writeInt(result.getNullable(i));
    }
}
