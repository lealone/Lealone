/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import org.lealone.db.session.ServerSession;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.session.SessionCancelStatement;
import org.lealone.server.protocol.session.SessionClose;
import org.lealone.server.protocol.session.SessionSetAutoCommit;
import org.lealone.sql.PreparedSQLStatement;

class SessionPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.SESSION_CANCEL_STATEMENT, new CancelStatement());
        register(PacketType.SESSION_SET_AUTO_COMMIT, new SetAutoCommit());
        register(PacketType.SESSION_CLOSE, new Close());
    }

    private static class CancelStatement implements PacketHandler<SessionCancelStatement> {
        @Override
        public Packet handle(ServerSession session, SessionCancelStatement packet) {
            PreparedSQLStatement command = (PreparedSQLStatement) session.removeCache(packet.statementId, true);
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
        public Packet handle(ServerSession session, SessionSetAutoCommit packet) {
            session.setAutoCommit(packet.autoCommit);
            return null;
        }
    }

    private static class Close implements PacketHandler<SessionClose> {
        @Override
        public Packet handle(PacketDeliveryTask task, SessionClose packet) {
            task.conn.closeSession(task.packetId, task.sessionId);
            return null;
        }
    }
}
