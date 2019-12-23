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

import java.sql.Statement;
import java.util.List;

import org.lealone.db.CommandParameter;
import org.lealone.db.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.server.TcpServerConnection;
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
                    results[i] = command.executeUpdate();
                } catch (Exception e) {
                    results[i] = Statement.EXECUTE_FAILED;
                }
            }
            return new BatchStatementUpdateAck(size, results);
        }
    }

    private static class PreparedUpdate implements PacketHandler<BatchStatementPreparedUpdate> {
        @Override
        public Packet handle(TcpServerConnection conn, ServerSession session, BatchStatementPreparedUpdate packet) {
            int commandId = packet.commandId;
            int size = packet.size;
            PreparedSQLStatement command = (PreparedSQLStatement) conn.getCache(commandId);
            List<? extends CommandParameter> params = command.getParameters();
            int[] results = new int[size];
            for (int i = 0; i < size; i++) {
                Value[] values = packet.batchParameters.get(i);
                for (int j = 0; j < values.length; j++) {
                    CommandParameter p = params.get(j);
                    p.setValue(values[j]);
                }
                try {
                    results[i] = command.executeUpdate();
                } catch (Exception e) {
                    results[i] = Statement.EXECUTE_FAILED;
                }
            }
            return new BatchStatementUpdateAck(size, results);
        }
    }
}
