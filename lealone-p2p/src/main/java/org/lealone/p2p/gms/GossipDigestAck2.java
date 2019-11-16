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
package org.lealone.p2p.gms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.lealone.net.NetNode;
import org.lealone.p2p.net.IVersionedSerializer;
import org.lealone.p2p.net.Message;
import org.lealone.p2p.net.MessageType;

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
 * last stage of the 3 way messaging of the Gossip protocol.
 */
public class GossipDigestAck2 implements Message<GossipDigestAck2> {
    public static final IVersionedSerializer<GossipDigestAck2> serializer = new GossipDigestAck2Serializer();

    final Map<NetNode, NodeState> epStateMap;

    GossipDigestAck2(Map<NetNode, NodeState> epStateMap) {
        this.epStateMap = epStateMap;
    }

    Map<NetNode, NodeState> getNodeStateMap() {
        return epStateMap;
    }

    @Override
    public MessageType getType() {
        return MessageType.GOSSIP_DIGEST_ACK2;
    }

    @Override
    public IVersionedSerializer<GossipDigestAck2> getSerializer() {
        return serializer;
    }

    private static class GossipDigestAck2Serializer implements IVersionedSerializer<GossipDigestAck2> {
        @Override
        public void serialize(GossipDigestAck2 ack2, DataOutput out, int version) throws IOException {
            out.writeInt(ack2.epStateMap.size());
            for (Map.Entry<NetNode, NodeState> entry : ack2.epStateMap.entrySet()) {
                NetNode ep = entry.getKey();
                ep.serialize(out);
                NodeState.serializer.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public GossipDigestAck2 deserialize(DataInput in, int version) throws IOException {
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
