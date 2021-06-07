/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.statement.StatementQuery;
import org.lealone.storage.PageKey;

public class DTransactionQuery extends StatementQuery {

    public final List<PageKey> pageKeys;

    public DTransactionQuery(List<PageKey> pageKeys, int resultId, int maxRows, int fetchSize, boolean scrollable,
            String sql) {
        super(resultId, maxRows, fetchSize, scrollable, sql);
        this.pageKeys = pageKeys;
    }

    public DTransactionQuery(NetInputStream in, int version) throws IOException {
        super(in, version);
        pageKeys = DTransactionUpdate.readPageKeys(in);
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
        DTransactionUpdate.writePageKeys(out, pageKeys);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionQuery> {
        @Override
        public DTransactionQuery decode(NetInputStream in, int version) throws IOException {
            return new DTransactionQuery(in, version);
        }
    }
}
