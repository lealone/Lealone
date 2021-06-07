/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.result;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.NoAckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class ResultChangeId implements NoAckPacket {

    public final int oldId;
    public final int newId;

    public ResultChangeId(int oldId, int newId) {
        this.oldId = oldId;
        this.newId = newId;
    }

    @Override
    public PacketType getType() {
        return PacketType.RESULT_CHANGE_ID;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(oldId).writeInt(newId);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ResultChangeId> {
        @Override
        public ResultChangeId decode(NetInputStream in, int version) throws IOException {
            return new ResultChangeId(in.readInt(), in.readInt());
        }
    }
}
