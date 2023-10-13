/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.Result;
import org.lealone.server.LinkableTask;
import org.lealone.server.PacketHandleTask;
import org.lealone.server.handler.PacketHandlers.QueryPacketHandler;
import org.lealone.server.handler.PacketHandlers.UpdatePacketHandler;
import org.lealone.server.protocol.QueryPacket;
import org.lealone.server.protocol.statement.StatementQuery;
import org.lealone.server.protocol.statement.StatementUpdate;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.StatementList;

// 用异步的方式执行StatementList
class StatementListPacketHandlers {

    final static Query queryHandler = new Query();
    final static Update updateHandler = new Update();

    static class Query extends QueryPacketHandler<StatementQuery> {

        private void handleAsyncResult(PacketHandleTask task, QueryPacket packet, AsyncResult<?> ar,
                AtomicInteger count, AtomicReference<Result> resultRef,
                AtomicReference<Throwable> causeRef) {
            if (ar.isFailed() && causeRef.get() == null)
                causeRef.set(ar.getCause());
            if (count.decrementAndGet() == 0) {
                if (causeRef.get() != null) {
                    task.conn.sendError(task.session, task.packetId, causeRef.get());
                } else {
                    sendResult(task, packet, resultRef.get());
                }
            }
        }

        @Override
        protected void createYieldableQuery(PacketHandleTask task, PreparedSQLStatement stmt,
                QueryPacket packet) {
            StatementList statementList = (StatementList) stmt;
            String[] statements = statementList.getRemaining().split(";");
            AtomicInteger count = new AtomicInteger();
            AtomicReference<Result> resultRef = new AtomicReference<>();
            AtomicReference<Throwable> causeRef = new AtomicReference<>();
            for (int i = 0; i < statements.length; i++) {
                statements[i] = statements[i].trim();
                if (!statements[i].isEmpty()) {
                    final String sql = statements[i];
                    LinkableTask subTask = new LinkableTask() {
                        @Override
                        public void run() {
                            PreparedSQLStatement command = task.session.prepareStatement(sql, -1);
                            PreparedSQLStatement.Yieldable<?> yieldable;
                            if (command.isQuery()) {
                                yieldable = command.createYieldableQuery(packet.maxRows,
                                        packet.scrollable, ar -> {
                                            handleAsyncResult(task, packet, ar, count, resultRef,
                                                    causeRef);
                                        });
                            } else {
                                yieldable = command.createYieldableUpdate(ar -> {
                                    handleAsyncResult(task, packet, ar, count, resultRef, causeRef);
                                });
                            }
                            task.si.submitYieldableCommand(task.packetId, yieldable);
                        }
                    };
                    count.incrementAndGet();
                    task.si.submitTasks(subTask);
                }
            }
            PreparedSQLStatement.Yieldable<?> yieldable = statementList.getFirstStatement()
                    .createYieldableQuery(packet.maxRows, packet.scrollable, ar -> {
                        if (ar.isSucceeded()) {
                            Result result = ar.getResult();
                            resultRef.set(result);
                        } else {
                            causeRef.set(ar.getCause());
                        }
                    });
            task.si.submitYieldableCommand(task.packetId, yieldable);
        }
    }

    static class Update extends UpdatePacketHandler<StatementUpdate> {

        private void handleAsyncResult(PacketHandleTask task, AsyncResult<?> ar, AtomicInteger count,
                AtomicReference<Integer> resultRef, AtomicReference<Throwable> causeRef) {
            if (ar.isFailed() && causeRef.get() == null)
                causeRef.set(ar.getCause());
            if (count.decrementAndGet() == 0) {
                if (causeRef.get() != null) {
                    task.conn.sendError(task.session, task.packetId, causeRef.get());
                } else {
                    task.conn.sendResponse(task, createAckPacket(task, resultRef.get()));
                }
            }
        }

        @Override
        protected void createYieldableUpdate(PacketHandleTask task, PreparedSQLStatement stmt) {
            StatementList statementList = (StatementList) stmt;
            String[] statements = statementList.getRemaining().split(";");
            AtomicInteger count = new AtomicInteger();
            AtomicReference<Integer> resultRef = new AtomicReference<>();
            AtomicReference<Throwable> causeRef = new AtomicReference<>();
            for (int i = 0; i < statements.length; i++) {
                statements[i] = statements[i].trim();
                if (!statements[i].isEmpty()) {
                    final String sql = statements[i];
                    LinkableTask subTask = new LinkableTask() {
                        @Override
                        public void run() {
                            PreparedSQLStatement command = task.session.prepareStatement(sql, -1);
                            PreparedSQLStatement.Yieldable<?> yieldable;
                            if (command.isQuery()) {
                                yieldable = command.createYieldableQuery(-1, false, ar -> {
                                    handleAsyncResult(task, ar, count, resultRef, causeRef);
                                });
                            } else {
                                yieldable = command.createYieldableUpdate(ar -> {
                                    handleAsyncResult(task, ar, count, resultRef, causeRef);
                                });
                            }
                            task.si.submitYieldableCommand(task.packetId, yieldable);
                        }
                    };
                    count.incrementAndGet();
                    task.si.submitTasks(subTask);
                }
            }

            PreparedSQLStatement.Yieldable<?> yieldable = statementList.getFirstStatement()
                    .createYieldableUpdate(ar -> {
                        if (ar.isSucceeded()) {
                            int updateCount = ar.getResult();
                            resultRef.set(updateCount);
                        } else {
                            causeRef.set(ar.getCause());
                        }
                    });
            task.si.submitYieldableCommand(task.packetId, yieldable);
        }
    }
}
