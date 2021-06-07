/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.statement;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class StatementUpdate implements Packet {

    public final String sql;

    public StatementUpdate(String sql) {
        this.sql = sql;
    }

    public StatementUpdate(NetInputStream in, int version) throws IOException {
        sql = in.readString();
    }

    @Override
    public PacketType getType() {
        return PacketType.STATEMENT_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STATEMENT_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(sql);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + sql + "]";
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StatementUpdate> {
        @Override
        public StatementUpdate decode(NetInputStream in, int version) throws IOException {
            return new StatementUpdate(in, version);
        }
    }
}
