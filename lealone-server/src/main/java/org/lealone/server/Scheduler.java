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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.concurrent.ScheduledExecutors;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.DateTimeUtils;
import org.lealone.db.Session;
import org.lealone.db.SessionStatus;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.AsyncTaskHandler;
import org.lealone.net.TransferConnection;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.transaction.Transaction;

public class Scheduler extends Thread
        implements SQLStatementExecutor, PageOperationHandler, AsyncTaskHandler, Transaction.Listener {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);

    static class PreparedCommand {
        private final TransferConnection conn;
        private final int packetId;
        private final Session session;
        private final PreparedSQLStatement stmt;
        private final PreparedSQLStatement.Yieldable<?> yieldable;
        private final SessionInfo si;

        PreparedCommand(TransferConnection conn, int packetId, Session session, PreparedSQLStatement stmt,
                PreparedSQLStatement.Yieldable<?> yieldable, SessionInfo si) {
            this.conn = conn;
            this.packetId = packetId;
            this.session = session;
            this.stmt = stmt;
            this.yieldable = yieldable;
            this.si = si;
        }

        void execute() {
            // 如果因为某些原因导致主动让出CPU，那么先放到队列末尾等待重新从中断处执行。
            if (yieldable.run()) {
                si.preparedCommands.add(this);
            }
        }
    }

    static class SessionInfo {
        // preparedCommands中的命令统一由scheduler调度执行
        private final Scheduler scheduler;
        private final ConcurrentLinkedQueue<PreparedCommand> preparedCommands;
        private final TcpServerConnection conn;
        private final int sessionTimeout;
        final Session session;
        final int sessionId;
        long last;

        SessionInfo(TcpServerConnection conn, Session session, int sessionId, int sessionTimeout) {
            scheduler = ScheduleService.getScheduler();
            preparedCommands = new ConcurrentLinkedQueue<>();
            this.conn = conn;
            this.session = session;
            this.sessionId = sessionId;
            this.sessionTimeout = sessionTimeout;
            updateLastTime();
            scheduler.addSessionInfo(this);
        }

        void updateLastTime() {
            last = System.currentTimeMillis();
        }

        void addCommand(PreparedCommand command) {
            // 如果即将被执行的命令也被分配到同样的线程中(scheduler)运行，
            // 那么就不需要放到队列中了直接执行即可。
            // TODO 如果command的优先级很低，立即执行它是否合适？
            if (scheduler == Thread.currentThread()) {
                // 同一个session中的上一个事务还在执行中，不能立刻执行它
                if (command.session.getStatus() == SessionStatus.COMMITTING_TRANSACTION) {
                    preparedCommands.add(command);
                } else {
                    command.execute();
                }
                return;
            }
            preparedCommands.add(command);
            scheduler.wakeUp();
        }

        void remove() {
            scheduler.removeSessionInfo(this);
        }

        Scheduler getScheduler() {
            return scheduler;
        }

        void checkSessionTimeout(long currentTime) {
            if (sessionTimeout <= 0)
                return;
            if (last + sessionTimeout < currentTime) {
                conn.closeSession(this);
                logger.warn("Client session timeout, session id: " + sessionId + ", host: "
                        + conn.getWritableChannel().getHost() + ", port: " + conn.getWritableChannel().getPort());
            }
        }
    }

    private final ConcurrentLinkedQueue<PageOperation> pageOperationQueue = new ConcurrentLinkedQueue<>();
    private final CopyOnWriteArrayList<SessionInfo> sessions = new CopyOnWriteArrayList<>();

    private final ConcurrentLinkedQueue<AsyncTask> minPriorityQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<AsyncTask> normPriorityQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<AsyncTask> maxPriorityQueue = new ConcurrentLinkedQueue<>();

    // 这个只增不删所以用CopyOnWriteArrayList
    private final CopyOnWriteArrayList<AsyncTask> periodicQueue = new CopyOnWriteArrayList<>();

    private final Semaphore haveWork = new Semaphore(1);
    private final long loopInterval;
    private boolean stop;
    private int nested;
    private PreparedCommand nextBestCommand;

    public Scheduler(int id, Map<String, String> config) {
        super(ScheduleService.class.getSimpleName() + "-" + id);
        setDaemon(true);
        // 默认100毫秒
        loopInterval = DateTimeUtils.getLoopInterval(config, "scheduler_loop_interval", 100);
    }

    private void addSessionInfo(SessionInfo si) {
        sessions.add(si);
    }

    private void removeSessionInfo(SessionInfo si) {
        sessions.remove(si);
    }

    @Override
    public void run() {
        // SQLEngineManager.getInstance().setSQLStatementExecutor(this);
        while (!stop) {
            runQueueTasks(maxPriorityQueue);
            runQueueTasks(normPriorityQueue);
            runQueueTasks(minPriorityQueue);

            runPageOperationTasks();
            executeNextStatement();

            // for (int i = 0, size = periodicQueue.size(); i < size; i++) {
            // periodicQueue.get(i).run();
            // }
        }
    }

    private void runQueueTasks(ConcurrentLinkedQueue<AsyncTask> queue) {
        Runnable task = queue.poll();
        while (task != null) {
            try {
                task.run();
            } catch (Throwable e) {
                logger.warn("Failed to run async task: " + task, e);
            }
            task = queue.poll();
        }
    }

    private void runPageOperationTasks() {
        PageOperation po = pageOperationQueue.poll();
        while (po != null) {
            try {
                po.run(this);
            } catch (Throwable e) {
                logger.warn("Failed to run page operation: " + po, e);
            }
            po = pageOperationQueue.poll();
        }
    }

    void end() {
        stop = true;
        wakeUp();
    }

    @Override
    public long getLoad() {
        return sessions.size();// maxPriorityQueue.size() + minPriorityQueue.size() + normPriorityQueue.size() +
                               // pageOperationQueue.size()
        // + sessions.size();
    }

    @Override
    public void handlePageOperation(PageOperation po) {
        pageOperationQueue.add(po);
        wakeUp();
    }

    @Override
    public void handle(AsyncTask task) {
        if (task.isPeriodic()) {
            periodicQueue.add(task);
        } else {
            switch (task.getPriority()) {
            case AsyncTask.NORM_PRIORITY:
                normPriorityQueue.add(task);
                break;
            case AsyncTask.MAX_PRIORITY:
                maxPriorityQueue.add(task);
                break;
            case AsyncTask.MIN_PRIORITY:
                minPriorityQueue.add(task);
                break;
            default:
                normPriorityQueue.add(task);
            }
        }
        wakeUp();
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(AsyncTask task, long initialDelay, long delay, TimeUnit unit) {
        return ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(task, initialDelay, delay, unit);
    }

    @Override
    public void executeNextStatement() {
        int priority = PreparedSQLStatement.MIN_PRIORITY;
        PreparedCommand last = null;
        while (true) {
            PreparedCommand c;
            if (nextBestCommand != null) {
                c = nextBestCommand;
                nextBestCommand = null;
            } else {
                c = getNextBestCommand(priority, true);
            }
            if (c == null) {
                checkSessionTimeout();
                runPageOperationTasks();
                runQueueTasks(maxPriorityQueue);
                runQueueTasks(normPriorityQueue);
                c = getNextBestCommand(priority, true);
                if (c == null) {
                    try {
                        haveWork.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
                        haveWork.drainPermits();
                    } catch (InterruptedException e) {
                        handleInterruptedException(e);
                    }
                    break;
                }
            }
            try {
                c.execute();
                // 说明没有新的命令了，一直在轮循
                if (last == c) {
                    runPageOperationTasks();
                    runQueueTasks(maxPriorityQueue);
                    runQueueTasks(normPriorityQueue);
                }
                last = c;
            } catch (Throwable e) {
                c.conn.sendError(c.session, c.packetId, e);
            }
        }
    }

    @Override
    public void executeNextStatementIfNeeded(PreparedSQLStatement current) {
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
                c.conn.sendError(c.session, c.packetId, e);
            }
        }

        if (hasHigherPriorityCommand) {
            current.setPriority(priority + 1);
        }
        nested--;
    }

    @Override
    public boolean yieldIfNeeded(PreparedSQLStatement current) {
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
        if (sessions.isEmpty())
            return null;

        ConcurrentLinkedQueue<PreparedCommand> bestQueue = null;

        for (SessionInfo si : sessions) {
            ConcurrentLinkedQueue<PreparedCommand> preparedCommands = si.preparedCommands;
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

    private void checkSessionTimeout() {
        if (sessions.isEmpty())
            return;
        long currentTime = System.currentTimeMillis();
        for (SessionInfo si : sessions) {
            si.checkSessionTimeout(currentTime);
        }
    }

    private void handleInterruptedException(InterruptedException e) {
        logger.warn(getName() + " is interrupted");
        end();
    }

    // 以下使用同步方式执行
    private AtomicInteger counter;
    private volatile RuntimeException e;

    @Override
    public void beforeOperation() {
        e = null;
        counter = new AtomicInteger(1);
        // logger.warn(getName() + " beforeOperation: " + counter.get());
    }

    @Override
    public void operationUndo() {
        counter.decrementAndGet();
        // logger.warn(getName() + " operationUndo: " + counter.get());
        wakeUp();
    }

    @Override
    public void operationComplete() {
        counter.decrementAndGet();
        // logger.warn(getName() + " operationComplete: " + counter.get());
        wakeUp();
    }

    @Override
    public void setException(RuntimeException e) {
        this.e = e;
    }

    @Override
    public RuntimeException getException() {
        return e;
    }

    @Override
    public void await() {
        for (;;) {
            if (counter.get() < 1)
                break;
            runQueueTasks(maxPriorityQueue);
            runQueueTasks(normPriorityQueue);
            runQueueTasks(minPriorityQueue);
            runPageOperationTasks();
            if (counter.get() < 1)
                break;
            try {
                haveWork.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
                haveWork.drainPermits();
            } catch (InterruptedException e) {
                handleInterruptedException(e);
            }
        }
        if (e != null)
            throw e;
    }
}
