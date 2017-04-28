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
package org.lealone.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.lealone.net.NetEndpoint;
import org.lealone.net.NetSerializer;

public class LeafPageMovePlan {

    public final String moverHostId;
    public final List<NetEndpoint> replicationEndpoints;
    public final ByteBuffer splitKey;
    private int index;

    public LeafPageMovePlan(String moverHostId, List<NetEndpoint> replicationEndpoints, ByteBuffer splitKey) {
        this.moverHostId = moverHostId;
        this.replicationEndpoints = replicationEndpoints;
        this.splitKey = splitKey;
    }

    public void incrementIndex() {
        index++;
    }

    public int getIndex() {
        return index;
    }

    public List<String> getReplicationEndpoints() {
        List<String> endpoints = new ArrayList<>(replicationEndpoints.size());
        for (NetEndpoint e : replicationEndpoints)
            endpoints.add(e.getHostAndPort());

        return endpoints;
    }

    public void serialize(NetSerializer netSerializer) throws IOException {
        netSerializer.writeInt(index).writeString(moverHostId).writeByteBuffer(splitKey.slice());
        netSerializer.writeInt(replicationEndpoints.size());
        for (NetEndpoint e : replicationEndpoints) {
            netSerializer.writeString(e.getHostAndPort());
        }
    }

    public static LeafPageMovePlan deserialize(NetSerializer netSerializer) throws IOException {
        int index = netSerializer.readInt();
        String moverHostId = netSerializer.readString();
        ByteBuffer splitKey = netSerializer.readByteBuffer();
        int size = netSerializer.readInt();
        ArrayList<NetEndpoint> replicationEndpoints = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            replicationEndpoints.add(NetEndpoint.createTCP(netSerializer.readString()));
        }
        LeafPageMovePlan plan = new LeafPageMovePlan(moverHostId, replicationEndpoints, splitKey);
        plan.index = index;
        return plan;
    }

}
