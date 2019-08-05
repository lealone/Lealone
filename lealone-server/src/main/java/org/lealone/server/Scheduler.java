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

import org.lealone.common.util.DateTimeUtils;
import org.lealone.db.Session;
import org.lealone.db.SessionStatus;
import org.lealone.net.Transfer;
import org.lealone.net.TransferPacketHandler;
import org.lealone.sql.PreparedStatement;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;

public class Scheduler extends Thread implements SQLStatementExecutor, TransferPacketHandler, PageOperationHandler {

    static class PreparedCommand {
        private final int id;
        private final PreparedStatement stmt;
        private final Transfer transfer;
        private final Session session;
        private final PreparedStatement.Yieldable<?> yieldable;
        CommandQueue queue;

        PreparedCommand(int id, PreparedStatement stmt, Transfer transfer, Session session,
                PreparedStatement.Yieldable<?> yieldable) {
            this.id = id;
            this.stmt = stmt;
            this.transfer = transfer;
            this.session = session;
            this.yieldable = yieldable;
        }

        void execute() {
            if (yieldable.run()) {
                queue.preparedCommands.add(this);
            }
        }
    }

    // 表示commands由commandHandler处理
    static class CommandQueue {
        final Scheduler scheduler;
        final ConcurrentLinkedQueue<PreparedCommand> preparedCommands;

        CommandQueue(Scheduler scheduler) {
            this.scheduler = scheduler;
            this.preparedCommands = new ConcurrentLinkedQueue<>();
        }
    }

    private final ConcurrentLinkedQueue<PageOperation> pageOperationQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Runnable> packetQueue = new ConcurrentLinkedQueue<>();
    private final CopyOnWriteArrayList<CommandQueue> commandQueues = new CopyOnWriteArrayList<>();

    private final Semaphore haveWork = new Semaphore(1);
    private final long loopInterval;
    private boolean stop;
    private int nested;
    private PreparedCommand nextBestCommand;

    Scheduler(int id, Map<String, String> config) {
        super(ScheduleService.class.getSimpleName() + "-" + id);
        setDaemon(false);
        // 默认100毫秒
        loopInterval = DateTimeUtils.getLoopInterval(config, "scheduler_loop_interval", 100);
    }

    void addCommandQueue(CommandQueue queue) {
        commandQueues.add(queue);
    }

    void removeCommandQueue(CommandQueue queue) {
        commandQueues.remove(queue);
    }

    @Override
    public void run() {
        // SQLEngineManager.getInstance().setSQLStatementExecutor(this);
        while (!stop) {
            Runnable task = packetQueue.poll();
            while (task != null) {
                task.run();
                task = packetQueue.poll();
            }

            PageOperation po = pageOperationQueue.poll();
            while (po != null) {
                po.run(this);
                po = pageOperationQueue.poll();
            }

            executeNextStatement();
        }
    }

    void end() {
        stop = true;
        wakeUp();
    }

    @Override
    public void handlePageOperation(PageOperation po) {
        pageOperationQueue.add(po);
        wakeUp();
    }

    @Override
    public void handlePacket(Runnable task) {
        packetQueue.add(task);
        wakeUp();
    }

    @Override
    public void executeNextStatement() {
        int priority = PreparedStatement.MIN_PRIORITY;
        while (true) {
            PreparedCommand c;
            if (nextBestCommand != null) {
                c = nextBestCommand;
                nextBestCommand = null;
            } else {
                c = getNextBestCommand(priority, true);
            }
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
                c.execute();
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
                c.execute();
            } catch (Throwable e) {
                c.transfer.getTransferConnection().sendError(c.transfer, c.id, e);
            }
        }

        if (hasHigherPriorityCommand) {
            current.setPriority(priority + 1);
        }
        nested--;
    }

    @Override
    public boolean yieldIfNeeded(PreparedStatement current) {
        // 如果来了更高优化级的命令，那么当前正在执行的语句就让出当前线程，当前线程转去执行高优先级的命令
        int priority = current.getPriority();
        nextBestCommand = getNextBestCommand(priority, false);
        if (nextBestCommand != null) {
            current.setPriority(priority + 1);
            return true;
        }
        return false;
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

    @Override
    public void wakeUp() {
        haveWork.release(1);
    }
}
