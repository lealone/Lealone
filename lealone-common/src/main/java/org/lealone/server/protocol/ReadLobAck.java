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

public class ReadLobAck implements AckPacket {

    public final byte[] buff;

    public ReadLobAck(byte[] buff) {
        this.buff = buff;
    }

    @Override
    public PacketType getType() {
        return PacketType.COMMAND_READ_LOB_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeBytes(buff);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<ReadLobAck> {
        @Override
        public ReadLobAck decode(NetInputStream in, int version) throws IOException {
            return new ReadLobAck(in.readBytes());
        }
    }
}
