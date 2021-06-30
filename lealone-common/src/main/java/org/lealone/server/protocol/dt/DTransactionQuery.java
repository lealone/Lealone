/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.statement.StatementQuery;

public class DTransactionQuery extends StatementQuery {

    public final DTransactionParameters parameters;

    public DTransactionQuery(int resultId, int maxRows, int fetchSize, boolean scrollable, String sql,
            DTransactionParameters parameters) {
        super(resultId, maxRows, fetchSize, scrollable, sql);
        this.parameters = parameters;
    }

    public DTransactionQuery(NetInputStream in, int version) throws IOException {
        super(in, version);
        parameters = new DTransactionParameters(in, version);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_QUERY;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_QUERY_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        parameters.encode(out, version);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionQuery> {
        @Override
        public DTransactionQuery decode(NetInputStream in, int version) throws IOException {
            return new DTransactionQuery(in, version);
        }
    }
}
