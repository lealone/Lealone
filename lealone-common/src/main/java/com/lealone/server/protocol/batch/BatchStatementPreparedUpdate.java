/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.lealone.db.value.Value;
import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class BatchStatementPreparedUpdate implements Packet {

    public final int commandId;
    public final int size;
    public final List<Value[]> batchParameterValues;

    public BatchStatementPreparedUpdate(int commandId, int size, List<Value[]> batchParameterValues) {
        this.commandId = commandId;
        this.size = size;
        this.batchParameterValues = batchParameterValues;
    }

    @Override
    public PacketType getType() {
        return PacketType.BATCH_STATEMENT_PREPARED_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.BATCH_STATEMENT_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(commandId);
        int size = batchParameterValues.size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            Value[] values = batchParameterValues.get(i);
            int len = values.length;
            out.writeInt(len);
            for (int j = 0; j < len; j++)
                out.writeValue(values[j]);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<BatchStatementPreparedUpdate> {
        @Override
        public BatchStatementPreparedUpdate decode(NetInputStream in, int version) throws IOException {
            int commandId = in.readInt();
            int size = in.readInt();
            ArrayList<Value[]> batchParameterValues = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int len = in.readInt();
                Value[] values = new Value[len];
                for (int j = 0; j < len; j++)
                    values[j] = in.readValue();
                batchParameterValues.add(values);
            }
            return new BatchStatementPreparedUpdate(commandId, size, batchParameterValues);
        }
    }
}
