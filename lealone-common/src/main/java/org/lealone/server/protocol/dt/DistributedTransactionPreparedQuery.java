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
import java.util.List;

import org.lealone.db.value.Value;
import org.lealone.net.NetInputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.ps.PreparedStatementQuery;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.storage.PageKey;

public class DistributedTransactionPreparedQuery extends PreparedStatementQuery {

    public DistributedTransactionPreparedQuery(List<PageKey> pageKeys, int resultId, int maxRows, int fetchSize,
            boolean scrollable, int commandId, int size, Value[] parameters) {
        super(pageKeys, resultId, maxRows, fetchSize, scrollable, commandId, size, parameters);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_QUERY;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_QUERY_ACK;
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DistributedTransactionPreparedQuery> {
        @Override
        public DistributedTransactionPreparedQuery decode(NetInputStream in, int version) throws IOException {
            List<PageKey> pageKeys = StatementUpdate.readPageKeys(in);
            int resultId = in.readInt();
            int maxRows = in.readInt();
            int fetchSize = in.readInt();
            boolean scrollable = in.readBoolean();
            int commandId = in.readInt();
            int size = in.readInt();
            Value[] parameters = new Value[size];
            for (int i = 0; i < size; i++)
                parameters[i] = in.readValue();
            return new DistributedTransactionPreparedQuery(pageKeys, resultId, maxRows, fetchSize, scrollable,
                    commandId, size, parameters);
        }
    }
}
