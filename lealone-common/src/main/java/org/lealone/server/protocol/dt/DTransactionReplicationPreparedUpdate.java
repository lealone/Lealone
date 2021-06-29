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
import org.lealone.server.protocol.replication.ReplicationPreparedUpdate;
import org.lealone.storage.PageKey;

public class DTransactionReplicationPreparedUpdate extends ReplicationPreparedUpdate {

    public final List<PageKey> pageKeys;
    public final String indexName;

    public DTransactionReplicationPreparedUpdate(int commandId, Value[] parameters, String replicationName,
            List<PageKey> pageKeys, String indexName) {
        super(commandId, parameters, replicationName);
        this.pageKeys = pageKeys;
        this.indexName = indexName;
    }

    public DTransactionReplicationPreparedUpdate(NetInputStream in, int version) throws IOException {
        super(in, version);
        pageKeys = DTransactionUpdate.readPageKeys(in);
        indexName = in.readString();
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_REPLICATION_PREPARED_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_REPLICATION_PREPARED_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        DTransactionUpdate.writePageKeys(out, pageKeys);
        out.writeString(indexName);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionReplicationPreparedUpdate> {
        @Override
        public DTransactionReplicationPreparedUpdate decode(NetInputStream in, int version) throws IOException {
            return new DTransactionReplicationPreparedUpdate(in, version);
        }
    }
}
