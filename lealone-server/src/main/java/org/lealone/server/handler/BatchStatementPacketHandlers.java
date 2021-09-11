/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.CommandParameter;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.server.PacketDeliveryTask;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketType;
import org.lealone.server.protocol.batch.BatchStatementPreparedUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdate;
import org.lealone.server.protocol.batch.BatchStatementUpdateAck;
import org.lealone.sql.PreparedSQLStatement;

//先把批量语句转成AsyncTask再按先后顺序一个个处理
class BatchStatementPacketHandlers extends PacketHandlers {

    static void register() {
        register(PacketType.BATCH_STATEMENT_UPDATE, new Update());
        register(PacketType.BATCH_STATEMENT_PREPARED_UPDATE, new PreparedUpdate());
    }

    private static class Update implements PacketHandler<BatchStatementUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, BatchStatementUpdate packet) {
            ServerSession session = task.session;
            int size = packet.size;
            int[] results = new int[size];
            AtomicInteger count = new AtomicInteger(size);
            AsyncTask[] subTasks = new AsyncTask[size];
            for (int i = 0; i < size; i++) {
                final int index = i;
                final String sql = packet.batchStatements.get(i);
                AsyncTask subTask = new AsyncTask() {
                    @Override
                    public void run() {
                        PreparedSQLStatement command = session.prepareStatement(sql, -1);
                        submitYieldableCommand(task, command, results, count, index);
                    }
                };
                subTasks[i] = subTask;
            }
            task.si.submitTasks(subTasks);
            return null;
        }
    }

    private static class PreparedUpdate implements PacketHandler<BatchStatementPreparedUpdate> {
        @Override
        public Packet handle(PacketDeliveryTask task, BatchStatementPreparedUpdate packet) {
            ServerSession session = task.session;
            int commandId = packet.commandId;
            int size = packet.size;
            PreparedSQLStatement command = (PreparedSQLStatement) session.getCache(commandId);
            List<? extends CommandParameter> params = command.getParameters();
            int[] results = new int[size];
            AtomicInteger count = new AtomicInteger(size);
            AsyncTask[] subTasks = new AsyncTask[size];
            for (int i = 0; i < size; i++) {
                final int index = i;
                Value[] values = packet.batchParameters.get(i);
                for (int j = 0; j < values.length; j++) {
                    CommandParameter p = params.get(j);
                    p.setValue(values[j]);
                }
                AsyncTask subTask = new AsyncTask() {
                    @Override
                    public void run() {
                        submitYieldableCommand(task, command, results, count, index);
                    }
                };
                subTasks[i] = subTask;
            }
            task.si.submitTasks(subTasks);
            return null;
        }
    }

    private static void submitYieldableCommand(PacketDeliveryTask task, PreparedSQLStatement command, int[] results,
            AtomicInteger count, int index) {
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
                task.conn.sendResponse(task, new BatchStatementUpdateAck(results.length, results));
            }
        });
        task.si.submitYieldableCommand(task.packetId, yieldable);
    }
}
