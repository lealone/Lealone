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

import java.util.List;

import org.lealone.db.CommandParameter;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.TcpServerConnection;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.ps.PreparedStatementClose;
import org.lealone.server.protocol.ps.PreparedStatementGetMetaData;
import org.lealone.server.protocol.ps.PreparedStatementGetMetaDataAck;
import org.lealone.server.protocol.ps.PreparedStatementPrepare;
import org.lealone.server.protocol.ps.PreparedStatementPrepareAck;
import org.lealone.server.protocol.ps.PreparedStatementPrepareReadParams;
import org.lealone.server.protocol.ps.PreparedStatementPrepareReadParamsAck;
import org.lealone.server.protocol.ps.PreparedStatementQuery;
import org.lealone.server.protocol.ps.PreparedStatementUpdate;
import org.lealone.sql.PreparedSQLStatement;

class PreparedStatementPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.PREPARED_STATEMENT_PREPARE, new Prepare());
        register(PacketType.PREPARED_STATEMENT_PREPARE_READ_PARAMS, new PrepareReadParams());
        register(PacketType.PREPARED_STATEMENT_QUERY, new PreparedQuery());
        register(PacketType.PREPARED_STATEMENT_UPDATE, new PreparedUpdate());
        register(PacketType.PREPARED_STATEMENT_GET_META_DATA, new GetMetaData());
        register(PacketType.PREPARED_STATEMENT_CLOSE, new Close());
    }

    private static class Prepare implements PacketHandler<PreparedStatementPrepare> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, PreparedStatementPrepare packet) {
            PreparedSQLStatement command = session.prepareStatement(packet.sql, -1);
            command.setId(packet.commandId);
            conn.addCache(packet.commandId, command);
            boolean isQuery = command.isQuery();
            return new PreparedStatementPrepareAck(isQuery);
        }
    }

    private static class PrepareReadParams implements PacketHandler<PreparedStatementPrepareReadParams> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session,
                PreparedStatementPrepareReadParams packet) {
            PreparedSQLStatement command = session.prepareStatement(packet.sql, -1);
            command.setId(packet.commandId);
            conn.addCache(packet.commandId, command);
            boolean isQuery = command.isQuery();
            List<? extends CommandParameter> params = command.getParameters();
            return new PreparedStatementPrepareReadParamsAck(isQuery, params);
        }
    }

    private static class PreparedQuery extends PreparedQueryPacketHandler<PreparedStatementQuery> {
        @Override
        public Packet handle(PacketDeliveryTask task, PreparedStatementQuery packet) {
            return handlePacket(task, packet);
        }
    }

    private static class PreparedUpdate extends PreparedUpdatePacketHandler<PreparedStatementUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, PreparedStatementUpdate packet) {
            return handlePacket(task, packet);
        }
    }

    private static class GetMetaData implements PacketHandler<PreparedStatementGetMetaData> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, PreparedStatementGetMetaData packet) {
            PreparedSQLStatement command = (PreparedSQLStatement) conn.getCache(packet.commandId);
            Result result = command.getMetaData();
            return new PreparedStatementGetMetaDataAck(result);
        }
    }

    private static class Close implements PacketHandler<PreparedStatementClose> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, PreparedStatementClose packet) {
            PreparedSQLStatement command = (PreparedSQLStatement) conn.removeCache(packet.commandId, true);
            if (command != null) {
                command.close();
            }
            return null;
        }
    }
}
