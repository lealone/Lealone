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
package org.lealone.net;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.db.SessionStatus;
import org.lealone.sql.SQLEngineManager;
import org.lealone.sql.SQLStatementExecutor;

public class CommandHandler extends Thread implements SQLStatementExecutor {

    private static final LinkedList<AsyncConnection> connections = new LinkedList<>();
    private static final int commandHandlersCount = 1; // Runtime.getRuntime().availableProcessors();
    private static final CommandHandler[] commandHandlers = new CommandHandler[commandHandlersCount];
    private static final AtomicInteger index = new AtomicInteger(0);

    public static void startCommandHandlers() {
        for (int i = 0; i < commandHandlersCount; i++) {
            commandHandlers[i] = new CommandHandler(i);
        }

        SQLEngineManager.getInstance().setSQLStatementExecutors(commandHandlers);
        for (int i = 0; i < commandHandlersCount; i++) {
            commandHandlers[i].start();
        }
    }

    public static void stopCommandHandlers() {
        for (int i = 0; i < commandHandlersCount; i++) {
            commandHandlers[i].end();
        }

        for (int i = 0; i < commandHandlersCount; i++) {
            try {
                commandHandlers[i].join();
            } catch (InterruptedException e) {
            }
        }
    }

    static CommandHandler getNextCommandHandler() {
        return commandHandlers[index.getAndIncrement() % commandHandlers.length];
    }

    public static void addConnection(AsyncConnection c) {
        connections.add(c);
    }

    public static void removeConnection(AsyncConnection c) {
        connections.remove(c);
        c.close();
    }

    private final LinkedList<Integer> sessions = new LinkedList<>();
    private final Semaphore haveWork = new Semaphore(1);
    private boolean stop;

    void addSessionId(Integer sessionId) {
        synchronized (sessions) {
            sessions.add(sessionId);
        }
    }

    void removeSessionId(Integer sessionId) {
        synchronized (sessions) {
            sessions.remove(sessionId);
        }
    }

    public CommandHandler(int id) {
        super("CommandHandler-" + id);
    }

    @Override
    public void run() {
        // SQLEngineManager.getInstance().setSQLStatementExecutor(this);
        while (!stop) {
            executeNextStatement();
        }
    }

    public void end() {
        stop = true;
        ready();
    }

    @Override
    public void ready() {
        haveWork.release(1);
    }

    @Override
    public void executeNextStatement() {
        try {
            haveWork.tryAcquire(100, TimeUnit.MILLISECONDS);
            haveWork.drainPermits();
        } catch (InterruptedException e) {
            throw new AssertionError();
        }

        while (true) {
            PreparedCommand c = getNextBestCommand();
            if (c == null)
                break;
            try {
                c.run();
            } catch (Throwable e) {
                c.transfer.getAsyncConnection().sendError(c.transfer, c.id, e);
            }
        }
    }

    private PreparedCommand getNextBestCommand() {
        if (connections.isEmpty())
            return null;

        AsyncConnection ac;
        PreparedCommand pc;
        ConcurrentLinkedQueue<PreparedCommand> bestPreparedCommandQueue = null;
        double cost = 0.0;

        outer: for (int i = 0, size = connections.size(); i < size; i++) {
            ac = connections.get(i);
            for (Integer sessionId : sessions) {
                ConcurrentLinkedQueue<PreparedCommand> preparedCommandQueue = ac.getPreparedCommandQueue(sessionId);
                if (preparedCommandQueue == null) {
                    removeSessionId(sessionId);
                    continue;
                }
                pc = preparedCommandQueue.peek();
                if (pc == null)
                    continue;

                SessionStatus sessionStatus = pc.session.getStatus();
                if (sessionStatus == SessionStatus.TRANSACTION_NOT_COMMIT) {
                    bestPreparedCommandQueue = preparedCommandQueue;
                    break outer;
                } else if (sessionStatus == SessionStatus.COMMITTING_TRANSACTION) {
                    continue;
                }

                if (bestPreparedCommandQueue == null || pc.stmt.getCost() < cost) {
                    bestPreparedCommandQueue = preparedCommandQueue;
                    cost = pc.stmt.getCost();
                }
            }
        }

        if (bestPreparedCommandQueue == null)
            return null;

        return bestPreparedCommandQueue.poll();
    }

}
