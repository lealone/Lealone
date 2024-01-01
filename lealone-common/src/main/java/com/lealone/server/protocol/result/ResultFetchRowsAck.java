/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.result;

import java.io.IOException;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.result.Result;
import com.lealone.db.value.Value;
import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class ResultFetchRowsAck implements AckPacket {

    public final NetInputStream in;
    public final Result result;
    public final int count;

    public ResultFetchRowsAck(NetInputStream in) {
        this.in = in;
        this.result = null;
        this.count = 0;
    }

    public ResultFetchRowsAck(Result result, int count) {
        this.in = null;
        this.result = result;
        this.count = count;
    }

    @Override
    public PacketType getType() {
        return PacketType.RESULT_FETCH_ROWS_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        writeRow(out, result, count);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ResultFetchRowsAck> {
        @Override
        public ResultFetchRowsAck decode(NetInputStream in, int version) throws IOException {
            return new ResultFetchRowsAck(in);
        }
    }

    public static void writeRow(NetOutputStream out, Result result, int count) throws IOException {
        try {
            int visibleColumnCount = result.getVisibleColumnCount();
            for (int i = 0; i < count; i++) {
                if (result.next()) {
                    out.writeBoolean(true);
                    Value[] v = result.currentRow();
                    for (int j = 0; j < visibleColumnCount; j++) {
                        out.writeValue(v[j]);
                    }
                } else {
                    out.writeBoolean(false);
                    break;
                }
            }
        } catch (Throwable e) {
            // 如果取结果集的下一行记录时发生了异常，
            // 结果集包必须加一个结束标记，结果集包后面跟一个异常包。
            out.writeBoolean(false);
            throw DbException.convert(e);
        }
    }
}
