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
package org.lealone.aose.gms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.net.IVerbHandler;
import org.lealone.aose.net.MessageIn;
import org.lealone.aose.net.MessageOut;
import org.lealone.aose.net.MessagingService;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetEndpoint;

public class GossipDigestSynVerbHandler implements IVerbHandler<GossipDigestSyn> {
    private static final Logger logger = LoggerFactory.getLogger(GossipDigestSynVerbHandler.class);

    @Override
    public void doVerb(MessageIn<GossipDigestSyn> message, int id) {
        NetEndpoint from = message.from;
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestSynMessage from {}", from);
        if (!Gossiper.instance.isEnabled()) {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestSynMessage because gossip is disabled");
            return;
        }

        GossipDigestSyn gDigestMessage = message.payload;
        /* If the message is from a different cluster throw it away. */
        if (!gDigestMessage.clusterId.equals(ConfigDescriptor.getClusterName())) {
            logger.warn("ClusterName mismatch from {} {}!={}", from, gDigestMessage.clusterId,
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
        Map<NetEndpoint, EndpointState> deltaEpStateMap = new HashMap<>();
        Gossiper.instance.examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap);
        if (logger.isTraceEnabled())
            logger.trace("sending {} digests and {} deltas", deltaGossipDigestList.size(), deltaEpStateMap.size());
        MessageOut<GossipDigestAck> gDigestAckMessage = new MessageOut<>(MessagingService.Verb.GOSSIP_DIGEST_ACK,
                new GossipDigestAck(deltaGossipDigestList, deltaEpStateMap), GossipDigestAck.serializer);
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAckMessage to {}", from);
        MessagingService.instance().sendOneWay(gDigestAckMessage, from);
    }

    /*
     * First construct a map whose key is the endpoint in the GossipDigest and the value is the
     * GossipDigest itself. Then build a list of version differences i.e difference between the
     * version in the GossipDigest and the version in the local state for a given NetEndpoint.
     * Sort this list. Now loop through the sorted list and retrieve the GossipDigest corresponding
     * to the endpoint from the map that was initially constructed.
    */
    private void doSort(List<GossipDigest> gDigestList) {
        /* Construct a map of endpoint to GossipDigest. */
        Map<NetEndpoint, GossipDigest> epToDigestMap = new HashMap<>();
        for (GossipDigest gDigest : gDigestList) {
            epToDigestMap.put(gDigest.getEndpoint(), gDigest);
        }

        /*
         * These digests have their maxVersion set to the difference of the version
         * of the local EndpointState and the version found in the GossipDigest.
        */
        List<GossipDigest> diffDigests = new ArrayList<>(gDigestList.size());
        for (GossipDigest gDigest : gDigestList) {
            NetEndpoint ep = gDigest.getEndpoint();
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
            int version = (epState != null) ? Gossiper.instance.getMaxEndpointStateVersion(epState) : 0;
            int diffVersion = Math.abs(version - gDigest.getMaxVersion());
            diffDigests.add(new GossipDigest(ep, gDigest.getGeneration(), diffVersion));
        }

        gDigestList.clear();
        Collections.sort(diffDigests);
        int size = diffDigests.size();
        /*
         * Report the digests in descending order. This takes care of the endpoints
         * that are far behind w.r.t this local endpoint
        */
        for (int i = size - 1; i >= 0; --i) {
            gDigestList.add(epToDigestMap.get(diffDigests.get(i).getEndpoint()));
        }
    }
}
