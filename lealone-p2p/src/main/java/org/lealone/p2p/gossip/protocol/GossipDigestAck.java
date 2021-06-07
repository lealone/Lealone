/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
 * This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
 * node. This is the 2 stage of the 3 way messaging in the Gossip protocol.
 */
public class GossipDigestAck implements P2pPacket {

    final List<GossipDigest> gDigestList;
    final Map<NetNode, NodeState> epStateMap;

    public GossipDigestAck(List<GossipDigest> gDigestList, Map<NetNode, NodeState> epStateMap) {
        this.gDigestList = gDigestList;
        this.epStateMap = epStateMap;
    }

    public List<GossipDigest> getGossipDigestList() {
        return gDigestList;
    }

    public Map<NetNode, NodeState> getNodeStateMap() {
        return epStateMap;
    }

    @Override
    public PacketType getType() {
        return PacketType.P2P_GOSSIP_DIGEST_ACK;
    }

    @Override
    public void encode(NetOutputStream nout, int version) throws IOException {
        DataOutputStream out = ((TransferOutputStream) nout).getDataOutputStream();
        GossipDigestSerializationHelper.serialize(gDigestList, out, version);
        out.writeInt(epStateMap.size());
        for (Map.Entry<NetNode, NodeState> entry : epStateMap.entrySet()) {
            NetNode ep = entry.getKey();
            ep.serialize(out);
            NodeState.serializer.serialize(entry.getValue(), out, version);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<GossipDigestAck> {
        @Override
        public GossipDigestAck decode(NetInputStream nin, int version) throws IOException {
            DataInputStream in = ((TransferInputStream) nin).getDataInputStream();
            List<GossipDigest> gDigestList = GossipDigestSerializationHelper.deserialize(in, version);
            int size = in.readInt();
            Map<NetNode, NodeState> epStateMap = new HashMap<>(size);

            for (int i = 0; i < size; ++i) {
                NetNode ep = NetNode.deserialize(in);
                NodeState epState = NodeState.serializer.deserialize(in, version);
                epStateMap.put(ep, epState);
            }
            return new GossipDigestAck(gDigestList, epStateMap);
        }
    }
}
