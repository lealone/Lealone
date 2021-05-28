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
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.ps.PreparedStatementUpdate;
import org.lealone.storage.PageKey;

public class DTransactionPreparedUpdate extends PreparedStatementUpdate {

    public final List<PageKey> pageKeys;

    public DTransactionPreparedUpdate(List<PageKey> pageKeys, int commandId, Value[] parameters) {
        super(commandId, parameters);
        this.pageKeys = pageKeys;
    }

    public DTransactionPreparedUpdate(NetInputStream in, int version) throws IOException {
        super(in, version);
        pageKeys = DTransactionUpdate.readPageKeys(in);
    }

    @Override
    public PacketType getType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_UPDATE;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.DISTRIBUTED_TRANSACTION_PREPARED_UPDATE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        super.encode(out, version);
        DTransactionUpdate.writePageKeys(out, pageKeys);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<DTransactionPreparedUpdate> {
        @Override
        public DTransactionPreparedUpdate decode(NetInputStream in, int version) throws IOException {
            return new DTransactionPreparedUpdate(in, version);
        }
    }
}
