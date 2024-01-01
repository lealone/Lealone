/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.result;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class ResultFetchRows implements Packet {

    public final int resultId;
    public final int count;

    public ResultFetchRows(int resultId, int count) {
        this.resultId = resultId;
        this.count = count;
    }

    @Override
    public PacketType getType() {
        return PacketType.RESULT_FETCH_ROWS;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.RESULT_FETCH_ROWS_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(resultId).writeInt(count);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ResultFetchRows> {
        @Override
        public ResultFetchRows decode(NetInputStream in, int version) throws IOException {
            return new ResultFetchRows(in.readInt(), in.readInt());
        }
    }
}
