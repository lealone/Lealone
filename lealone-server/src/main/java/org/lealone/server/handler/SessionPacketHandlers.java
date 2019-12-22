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

import org.lealone.db.ServerSession;
import org.lealone.server.TcpServerConnection;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.session.SessionCancelStatement;
import org.lealone.server.protocol.session.SessionClose;
import org.lealone.server.protocol.session.SessionInit;
import org.lealone.server.protocol.session.SessionSetAutoCommit;
import org.lealone.sql.PreparedSQLStatement;

class SessionPacketHandlers extends PacketHandlers {

    static {
        register(PacketType.SESSION_INIT, new Init());
        register(PacketType.SESSION_CANCEL_STATEMENT, new CancelStatement());
        register(PacketType.SESSION_SET_AUTO_COMMIT, new SetAutoCommit());
        register(PacketType.SESSION_CLOSE, new Close());
    }

    // TODO
    private static class Init implements PacketHandler<SessionInit> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, SessionInit packet) {
            return null;
        }
    }

    private static class CancelStatement implements PacketHandler<SessionCancelStatement> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, SessionCancelStatement packet) {
            PreparedSQLStatement command = (PreparedSQLStatement) conn.removeCache(packet.statementId, false);
            if (command != null) {
                command.cancel();
                command.close();
            } else {
                session.cancelStatement(packet.statementId);
            }
            return null;
        }
    }

    private static class SetAutoCommit implements PacketHandler<SessionSetAutoCommit> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, SessionSetAutoCommit packet) {
            session.setAutoCommit(packet.autoCommit);
            return null;
        }
    }

    private static class Close implements PacketHandler<SessionClose> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, int sessionId, SessionClose packet,
                int packetId) {
            conn.closeSession(packetId, sessionId);
            return null;
        }
    }
}
