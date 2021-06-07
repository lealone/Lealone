/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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
