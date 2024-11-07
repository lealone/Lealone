/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.handler;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.lealone.db.async.AsyncResult;
import com.lealone.db.result.Result;
import com.lealone.server.handler.PacketHandlers.QueryPacketHandler;
import com.lealone.server.handler.PacketHandlers.UpdatePacketHandler;
import com.lealone.server.protocol.QueryPacket;
import com.lealone.server.protocol.statement.StatementQuery;
import com.lealone.server.protocol.statement.StatementUpdate;
import com.lealone.server.scheduler.LinkableTask;
import com.lealone.server.scheduler.PacketHandleTask;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.StatementList;

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
                    task.sendError(causeRef.get());
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
                            task.submitYieldableCommand(yieldable);
                        }
                    };
                    count.incrementAndGet();
                    task.si().submitTask(subTask, true);
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
            task.submitYieldableCommand(yieldable);
        }
    }

    static class Update extends UpdatePacketHandler<StatementUpdate> {

        private void handleAsyncResult(PacketHandleTask task, AsyncResult<?> ar, AtomicInteger count,
                AtomicReference<Integer> resultRef, AtomicReference<Throwable> causeRef) {
            if (ar.isFailed() && causeRef.get() == null)
                causeRef.set(ar.getCause());
            if (count.decrementAndGet() == 0) {
                if (causeRef.get() != null) {
                    task.sendError(causeRef.get());
                } else {
                    task.sendResponse(createAckPacket(task, resultRef.get()));
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
                            task.submitYieldableCommand(yieldable);
                        }
                    };
                    count.incrementAndGet();
                    task.si().submitTask(subTask, true);
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
            task.submitYieldableCommand(yieldable);
        }
    }
}
