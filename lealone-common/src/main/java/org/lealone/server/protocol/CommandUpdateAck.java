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

public class CommandUpdateAck implements AckPacket {

    public final int updateCount;

    public CommandUpdateAck(int updateCount) {
        this.updateCount = updateCount;
    }

    @Override
    public PacketType getType() {
        return PacketType.COMMAND_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(updateCount);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<CommandUpdateAck> {
        @Override
        public CommandUpdateAck decode(NetInputStream in, int version) throws IOException {
            return new CommandUpdateAck(in.readInt());
        }
    }
}
