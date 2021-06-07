/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.protocol;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class GossipResponse implements P2pPacket {

    @Override
    public PacketType getType() {
        return PacketType.P2P_REQUEST_RESPONSE;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<GossipResponse> {
        @Override
        public GossipResponse decode(NetInputStream in, int version) throws IOException {
            return new GossipResponse();
        }
    }
}
