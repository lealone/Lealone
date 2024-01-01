/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol.session;

import java.io.IOException;

import com.lealone.db.Constants;
import com.lealone.db.RunMode;
import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;
import com.lealone.server.protocol.AckPacket;
import com.lealone.server.protocol.PacketDecoder;
import com.lealone.server.protocol.PacketType;

public class SessionInitAck implements AckPacket {

    public final int clientVersion;
    public final boolean autoCommit;
    public final String targetNodes;
    public final RunMode runMode;
    public final boolean invalid;
    public final int consistencyLevel;

    public SessionInitAck(int clientVersion, boolean autoCommit, String targetNodes, RunMode runMode,
            boolean invalid, int consistencyLevel) {
        this.clientVersion = clientVersion;
        this.autoCommit = autoCommit;
        this.targetNodes = targetNodes;
        this.runMode = runMode;
        this.invalid = invalid;
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public PacketType getType() {
        return PacketType.SESSION_INIT_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(clientVersion);
        out.writeBoolean(autoCommit);
        if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
            out.writeString(targetNodes);
            out.writeString(runMode.toString());
            out.writeBoolean(invalid);
            out.writeInt(consistencyLevel);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<SessionInitAck> {
        @Override
        public SessionInitAck decode(NetInputStream in, int version) throws IOException {
            int clientVersion = in.readInt();
            boolean autoCommit = in.readBoolean();
            if (clientVersion >= Constants.TCP_PROTOCOL_VERSION_6) {
                String targetNodes = in.readString();
                RunMode runMode = RunMode.valueOf(in.readString());
                boolean invalid = in.readBoolean();
                int consistencyLevel = in.readInt();
                return new SessionInitAck(clientVersion, autoCommit, targetNodes, runMode, invalid,
                        consistencyLevel);
            } else {
                return new SessionInitAck(clientVersion, autoCommit, null, RunMode.CLIENT_SERVER, false,
                        0);
            }
        }
    }
}
