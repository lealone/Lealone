/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class BatchStatementUpdate implements Packet {

    public final int size;
    public final List<String> batchStatements;

    public BatchStatementUpdate(int size, List<String> batchStatements) {
        this.size = size;
        this.batchStatements = batchStatements;
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
            out.writeString(batchStatements.get(i));
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<BatchStatementUpdate> {
        @Override
        public BatchStatementUpdate decode(NetInputStream in, int version) throws IOException {
            int size = in.readInt();
            ArrayList<String> batchStatements = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                batchStatements.add(in.readString());
            return new BatchStatementUpdate(size, batchStatements);
        }
    }
}