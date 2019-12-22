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
package org.lealone.server.protocol.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class BatchStatementUpdate implements Packet {

    public final int size;
    public final List<String> batchStatements;

    public BatchStatementUpdate(int size, List<String> batchStatements) {
        this.size = size;
        this.batchStatements = batchStatements;
    }

    @Override
    public PacketType getType() {
        return PacketType.COMMAND_BATCH_STATEMENT_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.COMMAND_BATCH_STATEMENT_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeString(batchStatements.get(i));
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<BatchStatementUpdate> {
        @Override
        public BatchStatementUpdate decode(NetInputStream in, int version) throws IOException {
            int size = in.readInt();
            ArrayList<String> batchStatements = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                batchStatements.add(in.readString());
            return new BatchStatementUpdate(size, batchStatements);
        }
    }
}