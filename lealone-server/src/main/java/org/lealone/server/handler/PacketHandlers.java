/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import java.util.List;

import org.lealone.db.CommandParameter;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.QueryPacket;
import org.lealone.server.protocol.ps.PreparedStatementQuery;
import org.lealone.server.protocol.ps.PreparedStatementUpdate;
import org.lealone.server.protocol.statement.StatementQuery;
import org.lealone.server.protocol.statement.StatementQueryAck;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.server.protocol.statement.StatementUpdateAck;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.storage.page.PageKey;

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
    }

    private static abstract class UpdateBase<P extends Packet> implements PacketHandler<P> {

        protected void createYieldableUpdate(PacketDeliveryTask task, PreparedSQLStatement stmt,
                List<PageKey> pageKeys) {
            PreparedSQLStatement.Yieldable<?> yieldable = stmt.createYieldableUpdate(ar -> {
                if (ar.isSucceeded()) {
                    int updateCount = ar.getResult();
                    task.conn.sendResponse(task, createAckPacket(task, updateCount));
                } else {
                    task.conn.sendError(task.session, task.packetId, ar.getCause());
                }
            });
            task.si.submitYieldableCommand(task.packetId, yieldable);
        }

        protected Packet createAckPacket(PacketDeliveryTask task, int updateCount) {
            return new StatementUpdateAck(updateCount);
        }
    }

    static abstract class UpdatePacketHandler<P extends StatementUpdate> extends UpdateBase<P> {

        protected Packet handlePacket(PacketDeliveryTask task, StatementUpdate packet) {
            return handlePacket(task, packet, null);
        }

        protected Packet handlePacket(PacketDeliveryTask task, StatementUpdate packet, List<PageKey> pageKeys) {
            PreparedSQLStatement stmt = prepareStatement(task, packet.sql, -1);
            createYieldableUpdate(task, stmt, pageKeys);
            return null;
        }
    }

    static abstract class PreparedUpdatePacketHandler<P extends PreparedStatementUpdate> extends UpdateBase<P> {

        protected Packet handlePacket(PacketDeliveryTask task, PreparedStatementUpdate packet) {
            return handlePacket(task, packet, null);
        }

        protected Packet handlePacket(PacketDeliveryTask task, PreparedStatementUpdate packet, List<PageKey> pageKeys) {
            PreparedSQLStatement stmt = getPreparedSQLStatementFromCache(task, packet.commandId, packet.parameters);
            createYieldableUpdate(task, stmt, pageKeys);
            return null;
        }
    }

    private static abstract class QueryBase<P extends QueryPacket> implements PacketHandler<P> {

        protected void createYieldableQuery(PacketDeliveryTask task, PreparedSQLStatement stmt, QueryPacket packet,
                List<PageKey> pageKeys) {

            PreparedSQLStatement.Yieldable<?> yieldable = stmt.createYieldableQuery(packet.maxRows, packet.scrollable,
                    ar -> {
                        if (ar.isSucceeded()) {
                            Result result = ar.getResult();
                            sendResult(task, packet, result);
                        } else {
                            task.conn.sendError(task.session, task.packetId, ar.getCause());
                        }
                    });
            task.si.submitYieldableCommand(task.packetId, yieldable);
        }

        protected void sendResult(PacketDeliveryTask task, QueryPacket packet, Result result) {
            task.session.addCache(packet.resultId, result);
            try {
                int rowCount = result.getRowCount();
                int fetch = packet.fetchSize;
                if (rowCount != -1)
                    fetch = Math.min(rowCount, packet.fetchSize);
                task.conn.sendResponse(task, createAckPacket(task, result, rowCount, fetch));
            } catch (Exception e) {
                task.conn.sendError(task.session, task.packetId, e);
            }
        }

        protected Packet createAckPacket(PacketDeliveryTask task, Result result, int rowCount, int fetch) {
            return new StatementQueryAck(result, rowCount, fetch);
        }
    }

    static abstract class QueryPacketHandler<P extends StatementQuery> extends QueryBase<P> {

        protected Packet handlePacket(PacketDeliveryTask task, StatementQuery packet) {
            return handlePacket(task, packet, null);
        }

        protected Packet handlePacket(PacketDeliveryTask task, StatementQuery packet, List<PageKey> pageKeys) {
            PreparedSQLStatement stmt = prepareStatement(task, packet.sql, packet.fetchSize);
            createYieldableQuery(task, stmt, packet, pageKeys);
            return null;
        }
    }

    static abstract class PreparedQueryPacketHandler<P extends PreparedStatementQuery> extends QueryBase<P> {

        protected Packet handlePacket(PacketDeliveryTask task, PreparedStatementQuery packet) {
            return handlePacket(task, packet, null);
        }

        protected Packet handlePacket(PacketDeliveryTask task, PreparedStatementQuery packet, List<PageKey> pageKeys) {
            PreparedSQLStatement stmt = getPreparedSQLStatementFromCache(task, packet.commandId, packet.parameters);
            createYieldableQuery(task, stmt, packet, pageKeys);
            return null;
        }
    }

    private static PreparedSQLStatement prepareStatement(PacketDeliveryTask task, String sql, int fetchSize) {
        PreparedSQLStatement stmt = task.session.prepareStatement(sql, fetchSize);
        // 客户端的非Prepared语句不需要缓存，非Prepared语句执行一次就结束
        // 所以可以用packetId当唯一标识，一般用来执行客户端发起的取消操作
        stmt.setId(task.packetId);
        return stmt;
    }

    private static PreparedSQLStatement getPreparedSQLStatementFromCache(PacketDeliveryTask task, int commandId,
            Value[] parameters) {
        PreparedSQLStatement stmt = (PreparedSQLStatement) task.session.getCache(commandId);
        List<? extends CommandParameter> params = stmt.getParameters();
        for (int i = 0, size = parameters.length; i < size; i++) {
            CommandParameter p = params.get(i);
            p.setValue(parameters[i]);
        }
        return stmt;
    }
}
