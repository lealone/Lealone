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
package org.lealone.server.protocol.lob;

import java.io.IOException;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class LobRead implements Packet {

    public final long lobId;
    public final byte[] hmac;
    public final long offset;
    public final int length;

    public LobRead(long lobId, byte[] hmac, long offset, int length) {
        this.lobId = lobId;
        this.hmac = hmac;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public PacketType getType() {
        return PacketType.LOB_READ;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.LOB_READ_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeLong(lobId).writeBytes(hmac).writeLong(offset).writeInt(length);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<LobRead> {
        @Override
        public LobRead decode(NetInputStream in, int version) throws IOException {
            return new LobRead(in.readLong(), in.readBytes(), in.readLong(), in.readInt());
        }
    }
}
