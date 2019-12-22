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
package org.lealone.p2p.gossip.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetNode;
import org.lealone.net.NetOutputStream;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.p2p.gossip.NodeState;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
 * last stage of the 3 way messaging of the Gossip protocol.
 */
public class GossipDigestAck2 implements P2pPacket {

    final Map<NetNode, NodeState> epStateMap;

    public GossipDigestAck2(Map<NetNode, NodeState> epStateMap) {
        this.epStateMap = epStateMap;
    }

    public Map<NetNode, NodeState> getNodeStateMap() {
        return epStateMap;
    }

    @Override
    public PacketType getType() {
        return PacketType.P2P_GOSSIP_DIGEST_ACK2;
    }

    @Override
    public void encode(NetOutputStream nout, int version) throws IOException {
        DataOutputStream out = ((TransferOutputStream) nout).getDataOutputStream();
        out.writeInt(epStateMap.size());
        for (Map.Entry<NetNode, NodeState> entry : epStateMap.entrySet()) {
            NetNode ep = entry.getKey();
            ep.serialize(out);
            NodeState.serializer.serialize(entry.getValue(), out, version);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<GossipDigestAck2> {
        @Override
        public GossipDigestAck2 decode(NetInputStream nin, int version) throws IOException {
            DataInputStream in = ((TransferInputStream) nin).getDataInputStream();
            int size = in.readInt();
            Map<NetNode, NodeState> epStateMap = new HashMap<>(size);

            for (int i = 0; i < size; ++i) {
                NetNode ep = NetNode.deserialize(in);
                NodeState epState = NodeState.serializer.deserialize(in, version);
                epStateMap.put(ep, epState);
            }
            return new GossipDigestAck2(epStateMap);
        }
    }
}
