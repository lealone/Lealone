/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.dt;

import java.io.IOException;
import java.util.List;

import org.lealone.db.value.Value;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.ps.PreparedStatementQuery;
import org.lealone.storage.PageKey;

public class DTransactionPreparedQuery extends PreparedStatementQuery {

    public final List<PageKey> pageKeys;
    public final String indexName;

    public DTransactionPreparedQuery(int resultId, int maxRows, int fetchSize, boolean scrollable, int commandId,
            Value[] parameters, List<PageKey> pageKeys, String indexName) {
        super(resultId, maxRows, fetchSize, scrollable, commandId, parameters);
        this.pageKeys = pageKeys;
        this.indexName = indexName;
    }

    public DTransactionPreparedQuery(NetInputStream in, int version) throws IOException {
        super(in, version);
        pageKeys = DTransactionUpdate.readPageKeys(in);
        indexName = in.readString();
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
        DTransactionUpdate.writePageKeys(out, pageKeys);
        out.writeString(indexName);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionPreparedQuery> {
        @Override
        public DTransactionPreparedQuery decode(NetInputStream in, int version) throws IOException {
            return new DTransactionPreparedQuery(in, version);
        }
    }
}
