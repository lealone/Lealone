/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class GossipDigestSerializationHelper {
    private GossipDigestSerializationHelper() {
    }

    static void serialize(List<GossipDigest> gDigestList, DataOutput out, int version) throws IOException {
        out.writeInt(gDigestList.size());
        for (GossipDigest gDigest : gDigestList)
            GossipDigest.serializer.serialize(gDigest, out, version);
    }

    static List<GossipDigest> deserialize(DataInput in, int version) throws IOException {
        int size = in.readInt();
        List<GossipDigest> gDigests = new ArrayList<GossipDigest>(size);
        for (int i = 0; i < size; ++i)
            gDigests.add(GossipDigest.serializer.deserialize(in, version));
        return gDigests;
    }
}