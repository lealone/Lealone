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
package org.lealone.server.protocol.statement;

import java.io.IOException;
import java.util.List;

import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;
import org.lealone.storage.PageKey;

public class StatementQuery implements Packet {

    public final List<PageKey> pageKeys;
    public final int resultId;
    public final int maxRows;
    public final int fetchSize;
    public final boolean scrollable;
    public final String sql;

    public StatementQuery(List<PageKey> pageKeys, int resultId, int maxRows, int fetchSize, boolean scrollable,
            String sql) {
        this.pageKeys = pageKeys;
        this.resultId = resultId;
        this.maxRows = maxRows;
        this.fetchSize = fetchSize;
        this.scrollable = scrollable;
        this.sql = sql;
    }

    @Override
    public PacketType getType() {
        return PacketType.STATEMENT_QUERY;
    }

    @Override
    public PacketType getAckType() {
        return PacketType.STATEMENT_QUERY_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        if (pageKeys == null) {
            out.writeInt(0);
        } else {
            int size = pageKeys.size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                PageKey pk = pageKeys.get(i);
                out.writePageKey(pk);
            }
        }
        out.writeInt(resultId).writeInt(maxRows).writeInt(fetchSize).writeBoolean(scrollable).writeString(sql);
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<StatementQuery> {
        @Override
        public StatementQuery decode(NetInputStream in, int version) throws IOException {
            List<PageKey> pageKeys = StatementUpdate.readPageKeys(in);
            int resultId = in.readInt();
            int maxRows = in.readInt();
            int fetchSize = in.readInt();
            boolean scrollable = in.readBoolean();
            String sql = in.readString();
            return new StatementQuery(pageKeys, resultId, maxRows, fetchSize, scrollable, sql);
        }
    }
}
