/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.handler;

import java.util.Map;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetNode;
import org.lealone.p2p.gossip.Gossiper;
import org.lealone.p2p.gossip.NodeState;
import org.lealone.p2p.gossip.protocol.GossipDigestAck2;
import org.lealone.p2p.gossip.protocol.P2pPacketIn;

public class GossipDigestAck2Handler implements P2pPacketHandler<GossipDigestAck2> {

    private static final Logger logger = LoggerFactory.getLogger(GossipDigestAck2Handler.class);

    @Override
    public void handle(P2pPacketIn<GossipDigestAck2> packetIn, int id) {
        if (logger.isTraceEnabled()) {
            NetNode from = packetIn.from;
            logger.trace("Received a GossipDigestAck2Message from {}", from);
        }
        if (!Gossiper.instance.isEnabled()) {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestAck2Message because gossip is disabled");
            return;
        }
        Map<NetNode, NodeState> remoteEpStateMap = packetIn.packet.getNodeStateMap();
        /* Notify the Failure Detector */
        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
        Gossiper.instance.applyStateLocally(remoteEpStateMap);
    }
}
