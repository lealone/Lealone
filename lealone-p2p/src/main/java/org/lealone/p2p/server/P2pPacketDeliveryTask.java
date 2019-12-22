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
package org.lealone.p2p.server;

import java.util.EnumSet;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.async.AsyncTask;
import org.lealone.p2p.gossip.Gossiper;
import org.lealone.p2p.gossip.handler.P2pPacketHandler;
import org.lealone.p2p.gossip.handler.P2pPacketHandlers;
import org.lealone.p2p.gossip.protocol.GossipResponse;
import org.lealone.p2p.gossip.protocol.P2pPacketIn;
import org.lealone.p2p.gossip.protocol.P2pPacketOut;
import org.lealone.server.protocol.PacketType;

@SuppressWarnings({ "rawtypes", "unchecked" })
class P2pPacketDeliveryTask implements AsyncTask {
    private static final Logger logger = LoggerFactory.getLogger(P2pPacketDeliveryTask.class);

    private static final EnumSet<PacketType> GOSSIP_PACKETS = EnumSet.of(PacketType.P2P_GOSSIP_DIGEST_SYN,
            PacketType.P2P_GOSSIP_DIGEST_ACK, PacketType.P2P_GOSSIP_DIGEST_ACK2);

    private final P2pPacketIn packetIn;
    private final long constructionTime;
    private final int id;

    public P2pPacketDeliveryTask(P2pPacketIn packetIn, int id, long timestamp) {
        assert packetIn != null;
        this.packetIn = packetIn;
        this.id = id;
        constructionTime = timestamp;
    }

    @Override
    public int getPriority() {
        return MIN_PRIORITY; // 集群之间的消息处理不急迫，所以优先级最低
    }

    @Override
    public void run() {
        PacketType packetType = packetIn.packet.getType();
        if (MessagingService.DROPPABLE_PACKETS.contains(packetType)
                && System.currentTimeMillis() > constructionTime + packetIn.getTimeout()) {
            MessagingService.instance().incrementDroppedMessages(packetType);
            return;
        }

        P2pPacketHandler packetHandler = P2pPacketHandlers.getHandler(packetType);
        if (packetHandler == null) {
            if (logger.isDebugEnabled())
                logger.debug("Unknown packet type {}", packetType);
            return;
        }

        try {
            packetHandler.handle(packetIn, id);
        } catch (Throwable t) {
            if (packetIn.doCallbackOnFailure()) {
                P2pPacketOut response = new P2pPacketOut(new GossipResponse())
                        .withParameter(MessagingService.FAILURE_RESPONSE_PARAM, MessagingService.ONE_BYTE);
                MessagingService.instance().sendReply(response, id, packetIn.from);
            }
            throw t;
        }
        if (GOSSIP_PACKETS.contains(packetType))
            Gossiper.instance.setLastProcessedMessageAt(constructionTime);
    }
}
