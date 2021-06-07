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

public class ResultClose implements NoAckPacket {

    public final int resultId;

    public ResultClose(int resultId) {
        this.resultId = resultId;
    }

    @Override
    public PacketType getType() {
        return PacketType.RESULT_CLOSE;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(resultId);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ResultClose> {
        @Override
        public ResultClose decode(NetInputStream in, int version) throws IOException {
            return new ResultClose(in.readInt());
        }
    }
}
