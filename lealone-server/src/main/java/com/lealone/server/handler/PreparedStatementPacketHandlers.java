/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.handler;

import com.lealone.db.result.Result;
import com.lealone.db.session.ServerSession;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketType;
import com.lealone.server.protocol.ps.PreparedStatementClose;
import com.lealone.server.protocol.ps.PreparedStatementGetMetaData;
import com.lealone.server.protocol.ps.PreparedStatementGetMetaDataAck;
import com.lealone.server.protocol.ps.PreparedStatementPrepare;
import com.lealone.server.protocol.ps.PreparedStatementPrepareAck;
import com.lealone.server.protocol.ps.PreparedStatementPrepareReadParams;
import com.lealone.server.protocol.ps.PreparedStatementPrepareReadParamsAck;
import com.lealone.server.protocol.ps.PreparedStatementQuery;
import com.lealone.server.protocol.ps.PreparedStatementUpdate;
import com.lealone.server.scheduler.PacketHandleTask;
import com.lealone.sql.PreparedSQLStatement;

class PreparedStatementPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.PREPARED_STATEMENT_PREPARE, new Prepare());
        register(PacketType.PREPARED_STATEMENT_PREPARE_READ_PARAMS, new PrepareReadParams());
        register(PacketType.PREPARED_STATEMENT_QUERY, new PreparedQuery());
        register(PacketType.PREPARED_STATEMENT_UPDATE, new PreparedUpdate());
        register(PacketType.PREPARED_STATEMENT_GET_META_DATA, new GetMetaData());
        register(PacketType.PREPARED_STATEMENT_CLOSE, new Close());
    }

    private static PreparedSQLStatement prepareStatement(ServerSession session, int commandId,
            String sql) {
        PreparedSQLStatement command = session.prepareStatement(sql, -1);
        command.setId(commandId);
        session.addCache(commandId, command);
        return command;
    }

    private static class Prepare implements PacketHandler<PreparedStatementPrepare> {
        @Override
        public Packet handle(ServerSession session, PreparedStatementPrepare packet) {
            PreparedSQLStatement command = prepareStatement(session, packet.commandId, packet.sql);
            return new PreparedStatementPrepareAck(command.isQuery());
        }
    }

    private static class PrepareReadParams implements PacketHandler<PreparedStatementPrepareReadParams> {
        @Override
        public Packet handle(ServerSession session, PreparedStatementPrepareReadParams packet) {
            PreparedSQLStatement command = prepareStatement(session, packet.commandId, packet.sql);
            return new PreparedStatementPrepareReadParamsAck(command.isQuery(), command.getParameters());
        }
    }

    private static class PreparedQuery extends PreparedQueryPacketHandler<PreparedStatementQuery> {
        @Override
        public Packet handle(PacketHandleTask task, PreparedStatementQuery packet) {
            return handlePacket(task, packet);
        }
    }

    private static class PreparedUpdate extends PreparedUpdatePacketHandler<PreparedStatementUpdate> {
        @Override
        public Packet handle(PacketHandleTask task, PreparedStatementUpdate packet) {
            return handlePacket(task, packet);
        }
    }

    private static class GetMetaData implements PacketHandler<PreparedStatementGetMetaData> {
        @Override
        public Packet handle(ServerSession session, PreparedStatementGetMetaData packet) {
            PreparedSQLStatement command = (PreparedSQLStatement) session.getCache(packet.commandId);
            Result result = command.getMetaData().get();
            return new PreparedStatementGetMetaDataAck(result);
        }
    }

    private static class Close implements PacketHandler<PreparedStatementClose> {
        @Override
        public Packet handle(ServerSession session, PreparedStatementClose packet) {
            PreparedSQLStatement command = (PreparedSQLStatement) session.removeCache(packet.commandId,
                    true);
            if (command != null) {
                command.close();
            }
            return null;
        }
    }
}
