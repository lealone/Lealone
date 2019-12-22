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
