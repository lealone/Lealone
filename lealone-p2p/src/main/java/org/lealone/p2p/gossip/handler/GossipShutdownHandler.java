/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.handler;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.p2p.gossip.FailureDetector;
import org.lealone.p2p.gossip.Gossiper;
import org.lealone.p2p.gossip.protocol.GossipShutdown;
import org.lealone.p2p.gossip.protocol.P2pPacketIn;

public class GossipShutdownHandler implements P2pPacketHandler<GossipShutdown> {

    private static final Logger logger = LoggerFactory.getLogger(GossipShutdownHandler.class);

    @Override
    public void handle(P2pPacketIn<GossipShutdown> packetIn, int id) {
        if (!Gossiper.instance.isEnabled()) {
            if (logger.isDebugEnabled())
                logger.debug("Ignoring shutdown message from {} because gossip is disabled", packetIn.from);
            return;
        }
        FailureDetector.instance.forceConviction(packetIn.from);
    }
}