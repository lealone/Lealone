/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.batch;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class BatchStatementUpdateAck implements AckPacket {

    public final int size;
    public final int[] results;

    public BatchStatementUpdateAck(int size, int[] results) {
        this.size = size;
        this.results = results;
    }

    @Override
    public PacketType getType() {
        return PacketType.BATCH_STATEMENT_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.BATCH_STATEMENT_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeInt(results[i]);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<BatchStatementUpdateAck> {
        @Override
        public BatchStatementUpdateAck decode(NetInputStream in, int version) throws IOException {
            int size = in.readInt();
            int[] results = new int[size];
            for (int i = 0; i < size; i++)
                results[i] = in.readInt();
            return new BatchStatementUpdateAck(size, results);
        }
    }
}
