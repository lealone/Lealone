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
package org.lealone.server.handler;

import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.server.TcpServerConnection;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.result.ResultChangeId;
import org.lealone.server.protocol.result.ResultClose;
import org.lealone.server.protocol.result.ResultFetchRows;
import org.lealone.server.protocol.result.ResultFetchRowsAck;
import org.lealone.server.protocol.result.ResultReset;

class ResultPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.RESULT_FETCH_ROWS, new FetchRows());
        register(PacketType.RESULT_CHANGE_ID, new ChangeId());
        register(PacketType.RESULT_RESET, new Reset());
        register(PacketType.RESULT_CLOSE, new Close());
    }

    private static class FetchRows implements PacketHandler<ResultFetchRows> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, ResultFetchRows packet) {
            Result result = (Result) conn.getCache(packet.resultId);
            return new ResultFetchRowsAck(result, packet.count);
        }
    }

    private static class ChangeId implements PacketHandler<ResultChangeId> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, ResultChangeId packet) {
            AutoCloseable obj = conn.removeCache(packet.oldId, false);
            conn.addCache(packet.newId, obj);
            return null;
        }
    }

    private static class Reset implements PacketHandler<ResultReset> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, ResultReset packet) {
            Result result = (Result) conn.getCache(packet.resultId);
            result.reset();
            return null;
        }
    }

    private static class Close implements PacketHandler<ResultClose> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, ResultClose packet) {
            Result result = (Result) conn.removeCache(packet.resultId, true);
            if (result != null) {
                result.close();
            }
            return null;
        }
    }
}
