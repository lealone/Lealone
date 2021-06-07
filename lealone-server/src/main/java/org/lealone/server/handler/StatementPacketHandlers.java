/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.statement.StatementQuery;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.server.protocol.statement.StatementUpdateAck;

public class StatementPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.STATEMENT_QUERY, new Query());
        register(PacketType.STATEMENT_UPDATE, new Update());
    }

    private static class Query extends QueryPacketHandler<StatementQuery> {
        @Override
        public Packet handle(PacketDeliveryTask task, StatementQuery packet) {
            return handlePacket(task, packet);
        }
    }

    private static class Update extends UpdatePacketHandler<StatementUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, StatementUpdate packet) {
            return handlePacket(task, packet);
        }

        @Override
        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new StatementUpdateAck(updateCount);
        }
    }
}
