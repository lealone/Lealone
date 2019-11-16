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
import java.util.ArrayList;
import java.util.List;

import org.lealone.net.NetNode;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;

public class LeafPageMovePlan {

    public final String moverHostId;
    public final List<NetNode> replicationNodes;
    public final PageKey pageKey;
    private int index;

    public LeafPageMovePlan(String moverHostId, List<NetNode> replicationNodes, PageKey pageKey) {
        this.moverHostId = moverHostId;
        this.replicationNodes = replicationNodes;
        this.pageKey = pageKey;
    }

    public void incrementIndex() {
        index++;
    }

    public int getIndex() {
        return index;
    }

    public List<String> getReplicationNodes() {
        List<String> nodes = new ArrayList<>(replicationNodes.size());
        for (NetNode e : replicationNodes)
            nodes.add(e.getHostAndPort());

        return nodes;
    }

    public void serialize(NetOutputStream out) throws IOException {
        out.writeInt(index).writeString(moverHostId).writePageKey(pageKey);
        out.writeInt(replicationNodes.size());
        for (NetNode e : replicationNodes) {
            out.writeString(e.getHostAndPort());
        }
    }

    public static LeafPageMovePlan deserialize(NetInputStream in) throws IOException {
        int index = in.readInt();
        String moverHostId = in.readString();
        PageKey pageKey = in.readPageKey();
        int size = in.readInt();
        ArrayList<NetNode> replicationNodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            replicationNodes.add(NetNode.createTCP(in.readString()));
        }
        LeafPageMovePlan plan = new LeafPageMovePlan(moverHostId, replicationNodes, pageKey);
        plan.index = index;
        return plan;
    }

}
