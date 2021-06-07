/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.handler;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.p2p.gossip.protocol.EchoPacket;
import org.lealone.p2p.gossip.protocol.GossipResponse;
import org.lealone.p2p.gossip.protocol.P2pPacketIn;
import org.lealone.p2p.gossip.protocol.P2pPacketOut;
import org.lealone.p2p.server.MessagingService;

public class EchoPacketHandler implements P2pPacketHandler<EchoPacket> {

    private static final Logger logger = LoggerFactory.getLogger(EchoPacketHandler.class);

    @Override
    public void handle(P2pPacketIn<EchoPacket> packetIn, int id) {
        P2pPacketOut<GossipResponse> echoResponse = new P2pPacketOut<>(new GossipResponse());
        if (logger.isTraceEnabled())
            logger.trace("Sending a EchoPacket reply {}", packetIn.from);
        MessagingService.instance().sendReply(echoResponse, id, packetIn.from);
    }
}
