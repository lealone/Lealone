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
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.storage.PageKey;

public class DistributedTransactionValidate implements Packet {

    public final String localTransactionName;

    public DistributedTransactionValidate(String localTransactionName) {
        this.localTransactionName = localTransactionName;
    }

    @Override
    public PacketType getType() {
        return PacketType.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeString(localTransactionName);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DistributedTransactionUpdate> {
        @Override
        public DistributedTransactionUpdate decode(NetInputStream in, int version) throws IOException {
            List<PageKey> pageKeys = CommandUpdate.readPageKeys(in);
            String sql = in.readString();
            return new DistributedTransactionUpdate(pageKeys, sql);
        }
    }
}
