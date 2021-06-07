/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol.session;

import java.io.IOException;

import org.lealone.db.RunMode;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class SessionInitAck implements AckPacket {

    public final int clientVersion;
    public final boolean autoCommit;
    public final String targetNodes;
    public final RunMode runMode;
    public final boolean invalid;

    public SessionInitAck(int clientVersion, boolean autoCommit, String targetNodes, RunMode runMode, boolean invalid) {
        this.clientVersion = clientVersion;
        this.autoCommit = autoCommit;
        this.targetNodes = targetNodes;
        this.runMode = runMode;
        this.invalid = invalid;
    }

    @Override
    public PacketType getType() {
        return PacketType.SESSION_INIT_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(clientVersion);
        out.writeBoolean(autoCommit);
        out.writeString(targetNodes);
        out.writeString(runMode.toString());
        out.writeBoolean(invalid);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<SessionInitAck> {
        @Override
        public SessionInitAck decode(NetInputStream in, int version) throws IOException {
            int clientVersion = in.readInt();
            boolean autoCommit = in.readBoolean();
            String targetNodes = in.readString();
            RunMode runMode = RunMode.valueOf(in.readString());
            boolean invalid = in.readBoolean();
            return new SessionInitAck(clientVersion, autoCommit, targetNodes, runMode, invalid);
        }
    }
}
