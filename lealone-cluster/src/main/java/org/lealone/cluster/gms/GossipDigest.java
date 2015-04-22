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
package org.lealone.cluster.gms;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;

import org.lealone.cluster.db.TypeSizes;
import org.lealone.cluster.io.DataOutputPlus;
import org.lealone.cluster.io.IVersionedSerializer;
import org.lealone.cluster.net.CompactEndpointSerializationHelper;

/**
 * Contains information about a specified list of Endpoints and the largest version
 * of the state they have generated as known by the local endpoint.
 */
public class GossipDigest implements Comparable<GossipDigest> {
    public static final IVersionedSerializer<GossipDigest> serializer = new GossipDigestSerializer();

    final InetAddress endpoint;
    final int generation;
    final int maxVersion;

    GossipDigest(InetAddress ep, int gen, int version) {
        endpoint = ep;
        generation = gen;
        maxVersion = version;
    }

    InetAddress getEndpoint() {
        return endpoint;
    }

    int getGeneration() {
        return generation;
    }

    int getMaxVersion() {
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
        sb.append(endpoint);
        sb.append(":");
        sb.append(generation);
        sb.append(":");
        sb.append(maxVersion);
        return sb.toString();
    }

    private static class GossipDigestSerializer implements IVersionedSerializer<GossipDigest> {
        @Override
        public void serialize(GossipDigest gDigest, DataOutputPlus out, int version) throws IOException {
            CompactEndpointSerializationHelper.serialize(gDigest.endpoint, out);
            out.writeInt(gDigest.generation);
            out.writeInt(gDigest.maxVersion);
        }

        @Override
        public GossipDigest deserialize(DataInput in, int version) throws IOException {
            InetAddress endpoint = CompactEndpointSerializationHelper.deserialize(in);
            int generation = in.readInt();
            int maxVersion = in.readInt();
            return new GossipDigest(endpoint, generation, maxVersion);
        }

        @Override
        public long serializedSize(GossipDigest gDigest, int version) {
            long size = CompactEndpointSerializationHelper.serializedSize(gDigest.endpoint);
            size += TypeSizes.NATIVE.sizeof(gDigest.generation);
            size += TypeSizes.NATIVE.sizeof(gDigest.maxVersion);
            return size;
        }
    }
}
