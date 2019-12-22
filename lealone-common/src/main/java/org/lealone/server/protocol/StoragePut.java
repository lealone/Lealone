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
package org.lealone.server.protocol;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;

public class StoragePut implements Packet {

    public final String mapName;
    public final byte[] key;
    public final byte[] value;
    public final boolean isDistributedTransaction;
    public final String replicationName;
    public final boolean raw;

    public StoragePut(String mapName, byte[] key, byte[] value, boolean isDistributedTransaction,
            String replicationName, boolean raw) {
        this.mapName = mapName;
        this.key = key;
        this.value = value;
        this.isDistributedTransaction = isDistributedTransaction;
        this.replicationName = replicationName;
        this.raw = raw;
    }

    @Override
    public PacketType getType() {
        return PacketType.COMMAND_STORAGE_PUT;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.COMMAND_STORAGE_PUT_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(mapName).writeBytes(key).writeBytes(value).writeBoolean(isDistributedTransaction)
                .writeString(replicationName).writeBoolean(raw);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StoragePut> {
        @Override
        public StoragePut decode(NetInputStream in, int version) throws IOException {
            return new StoragePut(in.readString(), in.readBytes(), in.readBytes(), in.readBoolean(), in.readString(),
                    in.readBoolean());
        }
    }
}
