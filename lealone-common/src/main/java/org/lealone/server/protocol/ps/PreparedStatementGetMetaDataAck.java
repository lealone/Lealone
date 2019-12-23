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
package org.lealone.server.protocol.ps;

import java.io.IOException;

import org.lealone.db.result.Result;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;
import org.lealone.server.protocol.AckPacket;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketType;

public class PreparedStatementGetMetaDataAck implements AckPacket {

    public final Result result;
    public final int columnCount;
    public final NetInputStream in;

    public PreparedStatementGetMetaDataAck(Result result) {
        this.result = result;
        columnCount = result.getVisibleColumnCount();
        in = null;
    }

    public PreparedStatementGetMetaDataAck(NetInputStream in, int columnCount) {
        result = null;
        this.columnCount = columnCount;
        this.in = in;
    }

    @Override
    public PacketType getType() {
        return PacketType.PREPARED_STATEMENT_GET_META_DATA_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeInt(columnCount);
        for (int i = 0; i < columnCount; i++) {
            writeColumn(out, result, i);
        }
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PreparedStatementGetMetaDataAck> {
        @Override
        public PreparedStatementGetMetaDataAck decode(NetInputStream in, int version) throws IOException {
            int columnCount = in.readInt();
            return new PreparedStatementGetMetaDataAck(in, columnCount);
        }
    }

    /**
    * Write a result column to the given output.
    *
    * @param result the result
    * @param i the column index
    */
    public static void writeColumn(NetOutputStream out, Result result, int i) throws IOException {
        out.writeString(result.getAlias(i));
        out.writeString(result.getSchemaName(i));
        out.writeString(result.getTableName(i));
        out.writeString(result.getColumnName(i));
        out.writeInt(result.getColumnType(i));
        out.writeLong(result.getColumnPrecision(i));
        out.writeInt(result.getColumnScale(i));
        out.writeInt(result.getDisplaySize(i));
        out.writeBoolean(result.isAutoIncrement(i));
        out.writeInt(result.getNullable(i));
    }
}
