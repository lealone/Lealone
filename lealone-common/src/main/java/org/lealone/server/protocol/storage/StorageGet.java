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
import java.nio.ByteBuffer;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class StorageGet implements Packet {

    public final String mapName;
    public final ByteBuffer key;
    public final boolean isDistributedTransaction;

    public StorageGet(String mapName, ByteBuffer key, boolean isDistributedTransaction) {
        this.mapName = mapName;
        this.key = key;
        this.isDistributedTransaction = isDistributedTransaction;
    }

    @Override
    public PacketType getType() {
        return PacketType.STORAGE_GET;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STORAGE_GET_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(mapName);
        out.writeByteBuffer(key);
        out.writeBoolean(isDistributedTransaction);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StorageGet> {
        @Override
        public StorageGet decode(NetInputStream in, int version) throws IOException {
            return new StorageGet(in.readString(), in.readByteBuffer(), in.readBoolean());
        }
    }
}
