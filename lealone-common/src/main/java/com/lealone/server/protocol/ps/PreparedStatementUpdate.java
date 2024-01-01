/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.ps;

import java.io.IOException;

import com.lealone.db.value.Value;
import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class PreparedStatementUpdate implements Packet {

    public final int commandId;
    public final Value[] parameters;

    public PreparedStatementUpdate(int commandId, Value[] parameters) {
        this.commandId = commandId;
        this.parameters = parameters;
    }

    public PreparedStatementUpdate(NetInputStream in, int version) throws IOException {
        commandId = in.readInt();
        int size = in.readInt();
        parameters = new Value[size];
        for (int i = 0; i < size; i++)
            parameters[i] = in.readValue();
    }

    @Override
    public PacketType getType() {
        return PacketType.PREPARED_STATEMENT_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STATEMENT_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        int size = parameters.length;
        out.writeInt(commandId);
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeValue(parameters[i]);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PreparedStatementUpdate> {
        @Override
        public PreparedStatementUpdate decode(NetInputStream in, int version) throws IOException {
            return new PreparedStatementUpdate(in, version);
        }
    }
}
