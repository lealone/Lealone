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
import org.lealone.db.session.Session;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.ps.PreparedStatementQuery;
import org.lealone.server.protocol.ps.PreparedStatementUpdate;
import org.lealone.server.protocol.statement.StatementQuery;
import org.lealone.server.protocol.statement.StatementQueryAck;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.server.protocol.statement.StatementUpdateAck;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.storage.PageKey;

@SuppressWarnings("rawtypes")
public class PacketHandlers {

    private static PacketHandler[] handlers = new PacketHandler[PacketType.VOID.value];

    public static void register(PacketType type, PacketHandler<? extends Packet> handler) {
        handlers[type.value] = handler;
    }

    public static PacketHandler getHandler(PacketType type) {
        return handlers[type.value];
    }

    public static PacketHandler getHandler(int type) {
        return handlers[type];
    }

    static {
        SessionPacketHandlers.register();
        PreparedStatementPacketHandlers.register();
        StatementPacketHandlers.register();
        BatchStatementPacketHandlers.register();
        ResultPacketHandlers.register();
        LobPacketHandlers.register();
        ReplicationPacketHandlers.register();
        DistributedTransactionPacketHandlers.register();
        StoragePacketHandlers.register();
    }

    static abstract class UpdatePacketHandler<P extends StatementUpdate> implements PacketHandler<P> {

        protected Packet handlePacket(PacketDeliveryTask task, StatementUpdate packet) {
            final Session session = task.session;

            // 客户端的非Prepared语句不需要缓存
            PreparedSQLStatement stmt = session.prepareStatement(packet.sql, -1);
            // 非Prepared语句执行一次就结束，所以可以用packetId当唯一标识，一般用来执行客户端发起的取消操作
            stmt.setId(task.packetId);

            PreparedSQLStatement.Yieldable<?> yieldable = stmt.createYieldableUpdate(ar -> {
                if (ar.isSucceeded()) {
                    int updateCount = ar.getResult();
                    task.conn.sendResponse(task, createAckPacket(task, updateCount));
                } else {
                    task.conn.sendError(session, task.packetId, ar.getCause());
                }
            });
            yieldable.setPageKeys(packet.pageKeys);
            task.conn.addPreparedCommandToQueue(task.packetId, task.si, stmt, yieldable);
            return null;
        }

        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new StatementUpdateAck(updateCount);
        }
    }

    static abstract class PreparedUpdatePacketHandler<P extends PreparedStatementUpdate> implements PacketHandler<P> {

        protected Packet handlePacket(PacketDeliveryTask task, PreparedStatementUpdate packet) {
            final Session session = task.session;

            List<PageKey> pageKeys = packet.pageKeys;
            PreparedSQLStatement stmt = (PreparedSQLStatement) task.conn.getCache(packet.commandId);
            List<? extends CommandParameter> params = stmt.getParameters();
            for (int i = 0; i < packet.size; i++) {
                CommandParameter p = params.get(i);
                p.setValue(packet.parameters[i]);
            }

            PreparedSQLStatement.Yieldable<?> yieldable = stmt.createYieldableUpdate(ar -> {
                if (ar.isSucceeded()) {
                    int updateCount = ar.getResult();
                    task.conn.sendResponse(task, createAckPacket(task, updateCount));
                } else {
                    task.conn.sendError(session, task.packetId, ar.getCause());
                }
            });
            yieldable.setPageKeys(pageKeys);
            task.conn.addPreparedCommandToQueue(task.packetId, task.si, stmt, yieldable);
            return null;
        }

        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new StatementUpdateAck(updateCount);
        }
    }

    static abstract class QueryPacketHandler<P extends StatementQuery> implements PacketHandler<P> {

        protected Packet handlePacket(PacketDeliveryTask task, StatementQuery packet) {
            final Session session = task.session;
            final int sessionId = task.sessionId;

            int resultId = packet.resultId;
            int maxRows = packet.maxRows;
            int fetchSize = packet.fetchSize;
            boolean scrollable = packet.scrollable;

            List<PageKey> pageKeys = packet.pageKeys;
            // 客户端的非Prepared语句不需要缓存
            PreparedSQLStatement stmt = session.prepareStatement(packet.sql, fetchSize);
            stmt.setId(task.packetId);
            stmt.setFetchSize(fetchSize);

            // 允许其他扩展跳过正常的流程
            if (executeQueryAsync(task.packetId, task.packetType, session, sessionId, stmt, resultId, fetchSize)) {
                return null;
            }
            PreparedSQLStatement.Yieldable<?> yieldable = stmt.createYieldableQuery(maxRows, scrollable, ar -> {
                if (ar.isSucceeded()) {
                    Result result = ar.getResult();
                    sendResult(task, task.packetId, task.packetType, session, sessionId, result, resultId, fetchSize);
                } else {
                    task.conn.sendError(session, task.packetId, ar.getCause());
                }
            });
            yieldable.setPageKeys(pageKeys);
            task.conn.addPreparedCommandToQueue(task.packetId, task.si, stmt, yieldable);
            return null;
        }

        protected boolean executeQueryAsync(int packetId, int packetType, Session session, int sessionId,
                PreparedSQLStatement stmt, int resultId, int fetchSize) {
            return false;
        }

        protected void sendResult(PacketDeliveryTask task, int packetId, int packetType, Session session, int sessionId,
                Result result, int resultId, int fetchSize) {
            task.conn.addCache(resultId, result);
            try {
                int rowCount = result.getRowCount();
                int fetch = fetchSize;
                if (rowCount != -1)
                    fetch = Math.min(rowCount, fetchSize);
                task.conn.sendResponse(task, createAckPacket(task, result, rowCount, fetch));
            } catch (Exception e) {
                task.conn.sendError(session, packetId, e);
            }
        }

        protected Packet createAckPacket(PacketDeliveryTask task, Result result, int rowCount, int fetch) {
            return new StatementQueryAck(result, rowCount, fetch);
        }
    }

    static abstract class PreparedQueryPacketHandler<P extends PreparedStatementQuery> implements PacketHandler<P> {

        protected Packet handlePacket(PacketDeliveryTask task, PreparedStatementQuery packet) {
            final Session session = task.session;
            final int sessionId = task.sessionId;

            int resultId = packet.resultId;
            int maxRows = packet.maxRows;
            int fetchSize = packet.fetchSize;
            boolean scrollable = packet.scrollable;

            List<PageKey> pageKeys = packet.pageKeys;
            PreparedSQLStatement stmt = (PreparedSQLStatement) task.conn.getCache(packet.commandId);
            List<? extends CommandParameter> params = stmt.getParameters();
            for (int i = 0; i < packet.size; i++) {
                CommandParameter p = params.get(i);
                p.setValue(packet.parameters[i]);
            }

            // 允许其他扩展跳过正常的流程
            if (executeQueryAsync(task.packetId, task.packetType, session, sessionId, stmt, resultId, fetchSize)) {
                return null;
            }
            PreparedSQLStatement.Yieldable<?> yieldable = stmt.createYieldableQuery(maxRows, scrollable, ar -> {
                if (ar.isSucceeded()) {
                    Result result = ar.getResult();
                    sendResult(task, task.packetId, task.packetType, session, sessionId, result, resultId, fetchSize);
                } else {
                    task.conn.sendError(session, task.packetId, ar.getCause());
                }
            });
            yieldable.setPageKeys(pageKeys);
            task.conn.addPreparedCommandToQueue(task.packetId, task.si, stmt, yieldable);
            return null;
        }

        protected boolean executeQueryAsync(int packetId, int packetType, Session session, int sessionId,
                PreparedSQLStatement stmt, int resultId, int fetchSize) {
            return false;
        }

        protected void sendResult(PacketDeliveryTask task, int packetId, int packetType, Session session, int sessionId,
                Result result, int resultId, int fetchSize) {
            task.conn.addCache(resultId, result);
            try {
                int rowCount = result.getRowCount();
                int fetch = fetchSize;
                if (rowCount != -1)
                    fetch = Math.min(rowCount, fetchSize);
                task.conn.sendResponse(task, createAckPacket(task, result, rowCount, fetch));
            } catch (Exception e) {
                task.conn.sendError(session, packetId, e);
            }
        }

        protected Packet createAckPacket(PacketDeliveryTask task, Result result, int rowCount, int fetch) {
            return new StatementQueryAck(result, rowCount, fetch);
        }
    }
}
