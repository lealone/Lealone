/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.db.result.Result;
import org.lealone.net.NetInputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class DTransactionPreparedQueryAck extends DTransactionQueryAck {

    public DTransactionPreparedQueryAck(Result result, int rowCount, int fetchSize) {
        super(result, rowCount, fetchSize);
    }

    public DTransactionPreparedQueryAck(NetInputStream in, int version) throws IOException {
        super(in, version);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_QUERY_ACK;
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionPreparedQueryAck> {
        @Override
        public DTransactionPreparedQueryAck decode(NetInputStream in, int version) throws IOException {
            return new DTransactionPreparedQueryAck(in, version);
        }
    }
}
