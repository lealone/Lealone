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
import org.lealone.server.protocol.statement.StatementUpdate;

public class DTransactionUpdate extends StatementUpdate {

    public final DTransactionParameters parameters;

    public DTransactionUpdate(String sql, DTransactionParameters parameters) {
        super(sql);
        this.parameters = parameters;
    }

    public DTransactionUpdate(NetInputStream in, int version) throws IOException {
        super(in, version);
        parameters = new DTransactionParameters(in, version);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        parameters.encode(out, version);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionUpdate> {
        @Override
        public DTransactionUpdate decode(NetInputStream in, int version) throws IOException {
            return new DTransactionUpdate(in, version);
        }
    }
}
