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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.db.SessionStatus;
import org.lealone.net.AsyncConnection.SessionInfo;
import org.lealone.sql.PreparedStatement;
import org.lealone.sql.SQLEngineManager;
import org.lealone.sql.SQLStatementExecutor;

public class CommandHandler extends Thread implements SQLStatementExecutor {

    private static final LinkedList<AsyncConnection> connections = new LinkedList<>();
    private static final int commandHandlersCount = 2; // Runtime.getRuntime().availableProcessors();
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

    private final AtomicLong sequence = new AtomicLong(0);
    private final ConcurrentHashMap<Long, SessionInfo> sessionInfoMap = new ConcurrentHashMap<>();
    private final Semaphore haveWork = new Semaphore(1);
    private boolean stop;
    private int nested;

    long getNextSequence() {
        return sequence.incrementAndGet();
    }

    void addSession(SessionInfo sessionInfo) {
        // SessionInfo的hostAndPort和sessionId字段不足以区别它自身，所以用commandHandlerSequence
        sessionInfoMap.put(sessionInfo.commandHandlerSequence, sessionInfo);
    }

    void removeSession(SessionInfo sessionInfo) {
        sessionInfoMap.remove(sessionInfo.commandHandlerSequence);
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
        wakeUp();
    }

    @Override
    public void wakeUp() {
        haveWork.release(1);
    }

    @Override
    public void executeNextStatement() {
        while (true) {
            PreparedCommand c = getNextBestCommand();
            if (c == null) {
                try {
                    haveWork.tryAcquire(100, TimeUnit.MILLISECONDS);
                    haveWork.drainPermits();
                } catch (InterruptedException e) {
                    throw new AssertionError();
                }
                break;
            }
            try {
                c.run();
            } catch (Throwable e) {
                c.transfer.getAsyncConnection().sendError(c.transfer, c.id, e);
            }
        }
    }

    private PreparedCommand getNextBestCommand() {
        if (sessionInfoMap.isEmpty())
            return null;

        ConcurrentLinkedQueue<PreparedCommand> bestPreparedCommandQueue = null;
        int priority = PreparedStatement.MIN_PRIORITY;

        for (SessionInfo sessionInfo : sessionInfoMap.values()) {
            ConcurrentLinkedQueue<PreparedCommand> preparedCommandQueue = sessionInfo.preparedCommandQueue;
            PreparedCommand pc = preparedCommandQueue.peek();
            if (pc == null)
                continue;

            SessionStatus sessionStatus = pc.session.getStatus();
            if (sessionStatus == SessionStatus.EXCLUSIVE_MODE) {
                continue;
            } else if (sessionStatus == SessionStatus.TRANSACTION_NOT_COMMIT) {
                bestPreparedCommandQueue = preparedCommandQueue;
                break;
            } else if (sessionStatus == SessionStatus.COMMITTING_TRANSACTION) {
                continue;
            }

            if (bestPreparedCommandQueue == null || pc.stmt.getPriority() > priority) {
                bestPreparedCommandQueue = preparedCommandQueue;
                priority = pc.stmt.getPriority();
            }
        }

        if (bestPreparedCommandQueue == null)
            return null;

        return bestPreparedCommandQueue.poll();
    }

    @Override
    public void executeNextStatementIfNeeded(PreparedStatement current) {
        // 如果出来各高优化级的命令，最多只抢占3次，避免堆栈溢出
        if (nested >= 3)
            return;
        nested++;
        int priority = current.getPriority();
        boolean hasHigherPriorityCommand = false;
        while (true) {
            PreparedCommand c = getNextBestCommand(priority);
            if (c == null) {
                break;
            }

            hasHigherPriorityCommand = true;
            try {
                c.run();
            } catch (Throwable e) {
                c.transfer.getAsyncConnection().sendError(c.transfer, c.id, e);
            }
        }

        if (hasHigherPriorityCommand) {
            current.setPriority(priority + 1);
        }
        nested--;
    }

    private PreparedCommand getNextBestCommand(int priority) {
        if (sessionInfoMap.isEmpty())
            return null;

        ConcurrentLinkedQueue<PreparedCommand> bestPreparedCommandQueue = null;

        for (SessionInfo sessionInfo : sessionInfoMap.values()) {
            ConcurrentLinkedQueue<PreparedCommand> preparedCommandQueue = sessionInfo.preparedCommandQueue;
            PreparedCommand pc = preparedCommandQueue.peek();
            if (pc == null)
                continue;

            // SessionStatus sessionStatus = pc.session.getStatus();
            // if (sessionStatus == SessionStatus.TRANSACTION_NOT_COMMIT) {
            // bestPreparedCommandQueue = preparedCommandQueue;
            // break;
            // } else if (sessionStatus == SessionStatus.COMMITTING_TRANSACTION) {
            // continue;
            // }

            if (pc.stmt.getPriority() > priority) {
                bestPreparedCommandQueue = preparedCommandQueue;
                priority = pc.stmt.getPriority();
            }
        }

        if (bestPreparedCommandQueue == null)
            return null;

        return bestPreparedCommandQueue.poll();
    }
}
