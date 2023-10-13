/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.ps;

import java.io.IOException;

import org.lealone.db.value.Value;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.QueryPacket;

public class PreparedStatementQuery extends QueryPacket {

    public final int commandId;
    public final Value[] parameters;

    public PreparedStatementQuery(int resultId, int maxRows, int fetchSize, boolean scrollable,
            int commandId, Value[] parameters) {
        super(resultId, maxRows, fetchSize, scrollable);
        this.commandId = commandId;
        this.parameters = parameters;
    }

    public PreparedStatementQuery(NetInputStream in, int version) throws IOException {
        super(in, version);
        commandId = in.readInt();
        int size = in.readInt();
        parameters = new Value[size];
        for (int i = 0; i < size; i++)
            parameters[i] = in.readValue();
    }

    @Override
    public PacketType getType() {
        return PacketType.PREPARED_STATEMENT_QUERY;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STATEMENT_QUERY_ACK; // 跟StatementQuery一样
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeInt(commandId);
        int size = parameters.length;
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeValue(parameters[i]);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PreparedStatementQuery> {
        @Override
        public PreparedStatementQuery decode(NetInputStream in, int version) throws IOException {
            return new PreparedStatementQuery(in, version);
        }
    }
}
