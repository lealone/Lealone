/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.statement;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.QueryPacket;

public class StatementQuery extends QueryPacket {

    public final String sql;

    public StatementQuery(int resultId, int maxRows, int fetchSize, boolean scrollable, String sql) {
        super(resultId, maxRows, fetchSize, scrollable);
        this.sql = sql;
    }

    public StatementQuery(NetInputStream in, int version) throws IOException {
        super(in, version);
        sql = in.readString();
    }

    @Override
    public PacketType getType() {
        return PacketType.STATEMENT_QUERY;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STATEMENT_QUERY_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        out.writeString(sql);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + sql + "]";
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StatementQuery> {
        @Override
        public StatementQuery decode(NetInputStream in, int version) throws IOException {
            return new StatementQuery(in, version);
        }
    }
}
