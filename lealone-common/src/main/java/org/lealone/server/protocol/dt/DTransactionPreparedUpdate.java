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
import org.lealone.server.protocol.ps.PreparedStatementUpdate;

public class DTransactionPreparedUpdate extends PreparedStatementUpdate {

    public final DTransactionParameters dtParameters;

    public DTransactionPreparedUpdate(int commandId, Value[] parameters, DTransactionParameters dtParameters) {
        super(commandId, parameters);
        this.dtParameters = dtParameters;
    }

    public DTransactionPreparedUpdate(NetInputStream in, int version) throws IOException {
        super(in, version);
        dtParameters = new DTransactionParameters(in, version);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        dtParameters.encode(out, version);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionPreparedUpdate> {
        @Override
        public DTransactionPreparedUpdate decode(NetInputStream in, int version) throws IOException {
            return new DTransactionPreparedUpdate(in, version);
        }
    }
}
