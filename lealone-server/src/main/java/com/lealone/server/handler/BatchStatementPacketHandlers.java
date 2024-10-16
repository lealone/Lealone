/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.handler;

import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.lealone.db.CommandParameter;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.server.protocol.Packet;
import com.lealone.server.protocol.PacketType;
import com.lealone.server.protocol.batch.BatchStatementPreparedUpdate;
import com.lealone.server.protocol.batch.BatchStatementUpdate;
import com.lealone.server.protocol.batch.BatchStatementUpdateAck;
import com.lealone.server.scheduler.LinkableTask;
import com.lealone.server.scheduler.PacketHandleTask;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.dml.MerSert;

//先把批量语句转成AsyncTask再按先后顺序一个个处理
class BatchStatementPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.BATCH_STATEMENT_UPDATE, new Update());
        register(PacketType.BATCH_STATEMENT_PREPARED_UPDATE, new PreparedUpdate());
    }

    private static class Update implements PacketHandler<BatchStatementUpdate> {
        @Override
        public Packet handle(PacketHandleTask task, BatchStatementUpdate packet) {
            ServerSession session = task.session;
            boolean autoCommit = session.isAutoCommit();
            if (autoCommit)
                session.setAutoCommit(false);
            int size = packet.size;
            int[] results = new int[size];
            AtomicInteger count = new AtomicInteger(size);
            task.si.updateLastActiveTime();
            for (int i = 0; i < size; i++) {
                final int index = i;
                final String sql = packet.batchStatements.get(i);
                LinkableTask subTask = new LinkableTask() {
                    @Override
                    public void run() {
                        PreparedSQLStatement command = session.prepareStatement(sql, -1);
                        submitYieldableCommand(task, command, results, count, index, autoCommit);
                    }
                };
                task.si.submitTask(subTask, false);
            }
            return null;
        }
    }

    private static class PreparedUpdate implements PacketHandler<BatchStatementPreparedUpdate> {
        @Override
        public Packet handle(PacketHandleTask task, BatchStatementPreparedUpdate packet) {
            PreparedSQLStatement command = (PreparedSQLStatement) task.session
                    .getCache(packet.commandId);
            if (command instanceof MerSert && ((MerSert) command).getQuery() == null)
                return handleMerSert(task, packet, (MerSert) command);
            else
                return handleOhter(task, packet, command);
        }

        private Packet handleMerSert(PacketHandleTask task, BatchStatementPreparedUpdate packet,
                MerSert command) {
            command.setBatchParameterValues(packet.batchParameterValues);
            LinkableTask subTask = new LinkableTask() {
                @Override
                public void run() {
                    PreparedSQLStatement.Yieldable<?> yieldable = command.createYieldableUpdate(ar -> {
                        command.setBatchParameterValues(null);
                        int size = packet.size;
                        int result = ar.isSucceeded() ? 1 : Statement.EXECUTE_FAILED;
                        int[] results = new int[size];
                        for (int i = 0; i < size; i++) {
                            results[i] = result;
                        }
                        task.conn.sendResponse(task,
                                new BatchStatementUpdateAck(results.length, results));
                    });
                    task.si.submitYieldableCommand(task.packetId, yieldable);
                }
            };
            task.si.submitTask(subTask);
            return null;
        }

        private Packet handleOhter(PacketHandleTask task, BatchStatementPreparedUpdate packet,
                PreparedSQLStatement command) {
            ServerSession session = task.session;
            boolean autoCommit = session.isAutoCommit();
            if (autoCommit)
                session.setAutoCommit(false);
            int size = packet.size;
            List<? extends CommandParameter> params = command.getParameters();
            int[] results = new int[size];
            AtomicInteger count = new AtomicInteger(size);
            task.si.updateLastActiveTime();
            for (int i = 0; i < size; i++) {
                final int index = i;
                final Value[] values = packet.batchParameterValues.get(i);
                LinkableTask subTask = new LinkableTask() {
                    @Override
                    public void run() {
                        // 不能放到外面设置，否则只取到最后一项
                        for (int j = 0; j < values.length; j++) {
                            CommandParameter p = params.get(j);
                            p.setValue(values[j]);
                        }
                        submitYieldableCommand(task, command, results, count, index, autoCommit);
                    }
                };
                task.si.submitTask(subTask, false);
            }
            return null;
        }
    }

    private static void submitYieldableCommand(PacketHandleTask task, PreparedSQLStatement command,
            int[] results, AtomicInteger count, int index, boolean autoCommit) {
        PreparedSQLStatement.Yieldable<?> yieldable = command.createYieldableUpdate(ar -> {
            if (ar.isSucceeded()) {
                int updateCount = ar.getResult();
                results[index] = updateCount;
            } else {
                // task.conn.sendError(task.session, task.packetId, ar.getCause());
                results[index] = Statement.EXECUTE_FAILED;
            }
            // 收到所有结果后再给客户端返回批量更新结果
            if (count.decrementAndGet() == 0) {
                if (autoCommit) {
                    task.session.asyncCommit(ar2 -> {
                        task.session.setAutoCommit(true);
                        task.conn.sendResponse(task,
                                new BatchStatementUpdateAck(results.length, results));
                    }, null);
                } else {
                    task.conn.sendResponse(task, new BatchStatementUpdateAck(results.length, results));
                }
            }
        });
        task.si.submitYieldableCommand(task.packetId, yieldable);
    }
}
