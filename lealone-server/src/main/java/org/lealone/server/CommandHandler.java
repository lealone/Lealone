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
package org.lealone.server;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.util.DateTimeUtils;
import org.lealone.db.SessionStatus;
import org.lealone.sql.PreparedStatement;
import org.lealone.sql.SQLEngineManager;
import org.lealone.sql.SQLStatementExecutor;

public class CommandHandler extends Thread implements SQLStatementExecutor {

    // 表示commands由commandHandler处理
    static class CommandQueue {
        final CommandHandler commandHandler;
        final ConcurrentLinkedQueue<PreparedCommand> preparedCommands;

        CommandQueue(CommandHandler commandHandler) {
            this.commandHandler = commandHandler;
            this.preparedCommands = new ConcurrentLinkedQueue<>();
        }
    }

    private static final int commandHandlersCount = 2; // Runtime.getRuntime().availableProcessors();
    private static final CommandHandler[] commandHandlers = new CommandHandler[commandHandlersCount];
    private static final AtomicInteger index = new AtomicInteger(0);

    public static void startCommandHandlers(Map<String, String> config) {
        for (int i = 0; i < commandHandlersCount; i++) {
            commandHandlers[i] = new CommandHandler(i, config);
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

    private final CopyOnWriteArrayList<CommandQueue> commandQueues = new CopyOnWriteArrayList<>();
    private final Semaphore haveWork = new Semaphore(1);
    private final long loopInterval;
    private boolean stop;
    private int nested;

    void addCommandQueue(CommandQueue queue) {
        commandQueues.add(queue);
    }

    void removeCommandQueue(CommandQueue queue) {
        commandQueues.remove(queue);
    }

    public CommandHandler(int id, Map<String, String> config) {
        super("CommandHandler-" + id);
        // setDaemon(true);
        // 默认100毫秒
        loopInterval = DateTimeUtils.getLoopInterval(config, "command_handler_loop_interval", 100);
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
        int priority = PreparedStatement.MIN_PRIORITY;
        while (true) {
            PreparedCommand c = getNextBestCommand(priority, true);
            if (c == null) {
                try {
                    haveWork.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
                    haveWork.drainPermits();
                } catch (InterruptedException e) {
                    throw new AssertionError();
                }
                break;
            }
            try {
                c.run();
            } catch (Throwable e) {
                c.transfer.getTransferConnection().sendError(c.transfer, c.id, e);
            }
        }
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
            PreparedCommand c = getNextBestCommand(priority, false);
            if (c == null) {
                break;
            }

            hasHigherPriorityCommand = true;
            try {
                c.run();
            } catch (Throwable e) {
                c.transfer.getTransferConnection().sendError(c.transfer, c.id, e);
            }
        }

        if (hasHigherPriorityCommand) {
            current.setPriority(priority + 1);
        }
        nested--;
    }

    private PreparedCommand getNextBestCommand(int priority, boolean checkStatus) {
        if (commandQueues.isEmpty())
            return null;

        ConcurrentLinkedQueue<PreparedCommand> bestQueue = null;

        for (CommandQueue commandQueue : commandQueues) {
            ConcurrentLinkedQueue<PreparedCommand> preparedCommands = commandQueue.preparedCommands;
            PreparedCommand pc = preparedCommands.peek();
            if (pc == null)
                continue;

            if (checkStatus) {
                SessionStatus sessionStatus = pc.session.getStatus();
                if (sessionStatus == SessionStatus.EXCLUSIVE_MODE) {
                    continue;
                } else if (sessionStatus == SessionStatus.TRANSACTION_NOT_COMMIT) {
                    bestQueue = preparedCommands;
                    break;
                } else if (sessionStatus == SessionStatus.COMMITTING_TRANSACTION) {
                    continue;
                }
                if (bestQueue == null) {
                    bestQueue = preparedCommands;
                }
            }

            if (pc.stmt.getPriority() > priority) {
                bestQueue = preparedCommands;
                priority = pc.stmt.getPriority();
            }
        }

        if (bestQueue == null)
            return null;

        return bestQueue.poll();
    }
}
