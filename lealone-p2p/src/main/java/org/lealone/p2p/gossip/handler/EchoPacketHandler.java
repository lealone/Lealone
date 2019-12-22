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
