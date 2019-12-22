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
package org.lealone.server.protocol.storage;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.storage.LeafPageMovePlan;

public class StoragePrepareMoveLeafPageAck implements AckPacket {

    public final LeafPageMovePlan leafPageMovePlan;

    public StoragePrepareMoveLeafPageAck(LeafPageMovePlan leafPageMovePlan) {
        this.leafPageMovePlan = leafPageMovePlan;
    }

    @Override
    public PacketType getType() {
        return PacketType.COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        leafPageMovePlan.serialize(out);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StoragePrepareMoveLeafPageAck> {
        @Override
        public StoragePrepareMoveLeafPageAck decode(NetInputStream in, int version) throws IOException {
            LeafPageMovePlan leafPageMovePlan = LeafPageMovePlan.deserialize(in);
            return new StoragePrepareMoveLeafPageAck(leafPageMovePlan);
        }
    }
}
