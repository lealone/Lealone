/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.session;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.NoAckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class SessionCancelStatement implements NoAckPacket {

    public final int statementId;

    public SessionCancelStatement(int statementId) {
        this.statementId = statementId;
    }

    @Override
    public PacketType getType() {
        return PacketType.SESSION_CANCEL_STATEMENT;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(statementId);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<SessionCancelStatement> {
        @Override
        public SessionCancelStatement decode(NetInputStream in, int version) throws IOException {
            return new SessionCancelStatement(in.readInt());
        }
    }
}
