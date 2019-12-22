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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.lealone.net.NetNode;

/**
 * Contains information about a specified list of Nodes and the largest version
 * of the state they have generated as known by the local node.
 */
public class GossipDigest implements Comparable<GossipDigest> {
    public static final IVersionedSerializer<GossipDigest> serializer = new GossipDigestSerializer();

    final NetNode node;
    final int generation;
    final int maxVersion;

    public GossipDigest(NetNode ep, int gen, int version) {
        node = ep;
        generation = gen;
        maxVersion = version;
    }

    public NetNode getNode() {
        return node;
    }

    public int getGeneration() {
        return generation;
    }

    public int getMaxVersion() {
        return maxVersion;
    }

    @Override
    public int compareTo(GossipDigest gDigest) {
        if (generation != gDigest.generation)
            return (generation - gDigest.generation);
        return (maxVersion - gDigest.maxVersion);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(node);
        sb.append(":");
        sb.append(generation);
        sb.append(":");
        sb.append(maxVersion);
        return sb.toString();
    }

    private static class GossipDigestSerializer implements IVersionedSerializer<GossipDigest> {
        @Override
        public void serialize(GossipDigest gDigest, DataOutput out, int version) throws IOException {
            gDigest.node.serialize(out);
            out.writeInt(gDigest.generation);
            out.writeInt(gDigest.maxVersion);
        }

        @Override
        public GossipDigest deserialize(DataInput in, int version) throws IOException {
            NetNode node = NetNode.deserialize(in);
            int generation = in.readInt();
            int maxVersion = in.readInt();
            return new GossipDigest(node, generation, maxVersion);
        }
    }
}
