/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.p2p.gossip.handler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetNode;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gossip.Gossiper;
import org.lealone.p2p.gossip.NodeState;
import org.lealone.p2p.gossip.protocol.GossipDigest;
import org.lealone.p2p.gossip.protocol.GossipDigestAck;
import org.lealone.p2p.gossip.protocol.GossipDigestSyn;
import org.lealone.p2p.gossip.protocol.P2pPacketIn;
import org.lealone.p2p.gossip.protocol.P2pPacketOut;
import org.lealone.p2p.server.MessagingService;

public class GossipDigestSynHandler implements P2pPacketHandler<GossipDigestSyn> {

    private static final Logger logger = LoggerFactory.getLogger(GossipDigestSynHandler.class);

    @Override
    public void handle(P2pPacketIn<GossipDigestSyn> packetIn, int id) {
        NetNode from = packetIn.from;
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestSynMessage from {}", from);
        if (!Gossiper.instance.isEnabled()) {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestSynMessage because gossip is disabled");
            return;
        }

        GossipDigestSyn gDigestMessage = packetIn.packet;
        /* If the message is from a different cluster throw it away. */
        if (!gDigestMessage.getClusterId().equals(ConfigDescriptor.getClusterName())) {
            logger.warn("ClusterName mismatch from {} {}!={}", from, gDigestMessage.getClusterId(),
                    ConfigDescriptor.getClusterName());
            return;
        }

        List<GossipDigest> gDigestList = gDigestMessage.getGossipDigests();
        if (logger.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (GossipDigest gDigest : gDigestList) {
                sb.append(gDigest);
                sb.append(" ");
            }
            logger.trace("Gossip syn digests are : {}", sb);
        }

        doSort(gDigestList);

        List<GossipDigest> deltaGossipDigestList = new ArrayList<>();
        Map<NetNode, NodeState> deltaEpStateMap = new HashMap<>();
        Gossiper.instance.examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap);
        if (logger.isTraceEnabled())
            logger.trace("sending {} digests and {} deltas", deltaGossipDigestList.size(), deltaEpStateMap.size());
        P2pPacketOut<GossipDigestAck> gDigestAckMessage = new P2pPacketOut<>(
                new GossipDigestAck(deltaGossipDigestList, deltaEpStateMap));
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAckMessage to {}", from);
        MessagingService.instance().sendOneWay(gDigestAckMessage, from);
    }

    /*
     * First construct a map whose key is the node in the GossipDigest and the value is the
     * GossipDigest itself. Then build a list of version differences i.e difference between the
     * version in the GossipDigest and the version in the local state for a given NetNode.
     * Sort this list. Now loop through the sorted list and retrieve the GossipDigest corresponding
     * to the node from the map that was initially constructed.
    */
    private void doSort(List<GossipDigest> gDigestList) {
        /* Construct a map of node to GossipDigest. */
        Map<NetNode, GossipDigest> epToDigestMap = new HashMap<>();
        for (GossipDigest gDigest : gDigestList) {
            epToDigestMap.put(gDigest.getNode(), gDigest);
        }

        /*
         * These digests have their maxVersion set to the difference of the version
         * of the local NodeState and the version found in the GossipDigest.
        */
        List<GossipDigest> diffDigests = new ArrayList<>(gDigestList.size());
        for (GossipDigest gDigest : gDigestList) {
            NetNode ep = gDigest.getNode();
            NodeState epState = Gossiper.instance.getNodeState(ep);
            int version = (epState != null) ? Gossiper.instance.getMaxNodeStateVersion(epState) : 0;
            int diffVersion = Math.abs(version - gDigest.getMaxVersion());
            diffDigests.add(new GossipDigest(ep, gDigest.getGeneration(), diffVersion));
        }

        gDigestList.clear();
        Collections.sort(diffDigests);
        int size = diffDigests.size();
        /*
         * Report the digests in descending order. This takes care of the nodes
         * that are far behind w.r.t this local node
        */
        for (int i = size - 1; i >= 0; --i) {
            gDigestList.add(epToDigestMap.get(diffDigests.get(i).getNode()));
        }
    }
}
