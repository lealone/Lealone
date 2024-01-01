/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.handler;

import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketType;
import com.lealone.server.protocol.statement.StatementQuery;
import com.lealone.server.protocol.statement.StatementUpdate;
import com.lealone.server.scheduler.PacketHandleTask;

public class StatementPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.STATEMENT_QUERY, new Query());
        register(PacketType.STATEMENT_UPDATE, new Update());
    }

    private static class Query extends QueryPacketHandler<StatementQuery> {
        @Override
        public Packet handle(PacketHandleTask task, StatementQuery packet) {
            return handlePacket(task, packet);
        }
    }

    private static class Update extends UpdatePacketHandler<StatementUpdate> {
        @Override
        public Packet handle(PacketHandleTask task, StatementUpdate packet) {
            return handlePacket(task, packet);
        }
    }
}
