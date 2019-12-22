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
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

/**
 * This is the first message that gets sent out as a start of the Gossip protocol in a
 * round.
 */
public class GossipDigestSyn implements P2pPacket {

    final String clusterId;
    final List<GossipDigest> gDigests;

    public GossipDigestSyn(String clusterId, List<GossipDigest> gDigests) {
        this.clusterId = clusterId;
        this.gDigests = gDigests;
    }

    public String getClusterId() {
        return clusterId;
    }

    public List<GossipDigest> getGossipDigests() {
        return gDigests;
    }

    @Override
    public PacketType getType() {
        return PacketType.P2P_GOSSIP_DIGEST_SYN;
    }

    @Override
    public void encode(NetOutputStream nout, int version) throws IOException {
        DataOutputStream out = ((TransferOutputStream) nout).getDataOutputStream();
        out.writeUTF(clusterId);
        GossipDigestSerializationHelper.serialize(gDigests, out, version);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<GossipDigestSyn> {
        @Override
        public GossipDigestSyn decode(NetInputStream nin, int version) throws IOException {
            DataInputStream in = ((TransferInputStream) nin).getDataInputStream();
            String clusterId = in.readUTF();
            List<GossipDigest> gDigests = GossipDigestSerializationHelper.deserialize(in, version);
            return new GossipDigestSyn(clusterId, gDigests);
        }
    }
}
