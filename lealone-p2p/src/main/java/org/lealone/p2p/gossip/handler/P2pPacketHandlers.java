/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.handler;

import java.util.HashMap;

import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;

public class P2pPacketHandlers {

    private static HashMap<Integer, P2pPacketHandler<? extends Packet>> handlers = new HashMap<>();

    public static void register(PacketType type, P2pPacketHandler<? extends Packet> decoder) {
        handlers.put(type.value, decoder);
    }

    public static P2pPacketHandler<? extends Packet> getHandler(PacketType type) {
        return handlers.get(type.value);
    }

    static {
        register(PacketType.P2P_ECHO, new EchoPacketHandler());
        register(PacketType.P2P_GOSSIP_DIGEST_SYN, new GossipDigestSynHandler());
        register(PacketType.P2P_GOSSIP_DIGEST_ACK, new GossipDigestAckHandler());
        register(PacketType.P2P_GOSSIP_DIGEST_ACK2, new GossipDigestAck2Handler());
        register(PacketType.P2P_GOSSIP_SHUTDOWN, new GossipShutdownHandler());
        register(PacketType.P2P_REQUEST_RESPONSE, new ResponsePacketHandler());
    }
}
