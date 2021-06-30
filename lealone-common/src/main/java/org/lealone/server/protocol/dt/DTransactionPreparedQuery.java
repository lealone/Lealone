/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.db.value.Value;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.ps.PreparedStatementQuery;

public class DTransactionPreparedQuery extends PreparedStatementQuery {

    public final DTransactionParameters dtParameters;

    public DTransactionPreparedQuery(int resultId, int maxRows, int fetchSize, boolean scrollable, int commandId,
            Value[] parameters, DTransactionParameters dtParameters) {
        super(resultId, maxRows, fetchSize, scrollable, commandId, parameters);
        this.dtParameters = dtParameters;
    }

    public DTransactionPreparedQuery(NetInputStream in, int version) throws IOException {
        super(in, version);
        dtParameters = new DTransactionParameters(in, version);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_QUERY;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_QUERY_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        dtParameters.encode(out, version);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionPreparedQuery> {
        @Override
        public DTransactionPreparedQuery decode(NetInputStream in, int version) throws IOException {
            return new DTransactionPreparedQuery(in, version);
        }
    }
}
