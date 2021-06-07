/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import java.sql.Statement;
import java.util.List;

import org.lealone.db.CommandParameter;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.batch.BatchStatementPreparedUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdateAck;
import org.lealone.sql.PreparedSQLStatement;

class BatchStatementPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.BATCH_STATEMENT_UPDATE, new Update());
        register(PacketType.BATCH_STATEMENT_PREPARED_UPDATE, new PreparedUpdate());
    }

    private static class Update implements PacketHandler<BatchStatementUpdate> {
        @Override
        public Packet handle(ServerSession session, BatchStatementUpdate packet) {
            int size = packet.size;
            int[] results = new int[size];
            for (int i = 0; i < size; i++) {
                String sql = packet.batchStatements.get(i);
                PreparedSQLStatement command = session.prepareStatement(sql, -1);
                try {
                    results[i] = command.executeUpdate().get();
                } catch (Exception e) {
                    results[i] = Statement.EXECUTE_FAILED;
                }
            }
            return new BatchStatementUpdateAck(size, results);
        }
    }

    private static class PreparedUpdate implements PacketHandler<BatchStatementPreparedUpdate> {
        @Override
        public Packet handle(ServerSession session, BatchStatementPreparedUpdate packet) {
            int commandId = packet.commandId;
            int size = packet.size;
            PreparedSQLStatement command = (PreparedSQLStatement) session.getCache(commandId);
            List<? extends CommandParameter> params = command.getParameters();
            int[] results = new int[size];
            for (int i = 0; i < size; i++) {
                Value[] values = packet.batchParameters.get(i);
                for (int j = 0; j < values.length; j++) {
                    CommandParameter p = params.get(j);
                    p.setValue(values[j]);
                }
                try {
                    results[i] = command.executeUpdate().get();
                } catch (Exception e) {
                    results[i] = Statement.EXECUTE_FAILED;
                }
            }
            return new BatchStatementUpdateAck(size, results);
        }
    }
}
