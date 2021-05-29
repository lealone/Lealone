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
package org.lealone.server.protocol.dt;

import java.io.IOException;

import org.lealone.db.result.Result;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.statement.StatementQueryAck;

public class DTransactionQueryAck extends StatementQueryAck {

    public final String localTransactionNames;

    public DTransactionQueryAck(Result result, int rowCount, int fetchSize, String localTransactionNames) {
        super(result, rowCount, fetchSize);
        this.localTransactionNames = localTransactionNames;
    }

    public DTransactionQueryAck(NetInputStream in, int version) throws IOException {
        super(in, version);
        localTransactionNames = in.readString();
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_QUERY_ACK;
    }

    @Override
    public void encodeExt(NetOutputStream out, int version) throws IOException {
        out.writeString(localTransactionNames);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionQueryAck> {
        @Override
        public DTransactionQueryAck decode(NetInputStream in, int version) throws IOException {
            return new DTransactionQueryAck(in, version);
        }
    }
}
