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
import java.util.concurrent.ConcurrentLinkedDeque;
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
import org.lealone.db.async.AsyncPeriodicTask;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.AsyncTaskHandler;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionStatus;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.transaction.Transaction;

public class Scheduler extends Thread
        implements SQLStatementExecutor, PageOperationHandler, AsyncTaskHandler, Transaction.Listener {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);

    private static class YieldableCommand {
        private final int packetId;
        private final SessionInfo si;
        private final PreparedSQLStatement stmt;
        private final PreparedSQLStatement.Yieldable<?> yieldable;

        YieldableCommand(int packetId, SessionInfo si, PreparedSQLStatement stmt,
                PreparedSQLStatement.Yieldable<?> yieldable) {
            this.packetId = packetId;
            this.si = si;
            this.stmt = stmt;
            this.yieldable = yieldable;
        }

        void execute() {
            // 如果因为某些原因导致主动让出CPU
            if (yieldable.run()) {
                // 必须放在队列前面，同一session中执行的语句是按顺序一条一条执行的，
                // 只有前一条语句执行完了才能执行下一条
                si.yieldableCommands.addFirst(this);
            }
        }
    }

    public static class SessionInfo {
        // yieldableCommands中的命令统一由scheduler调度执行
        private final Scheduler scheduler;
        // 只有用ConcurrentLinkedDeque才支持addFirst
        private final ConcurrentLinkedDeque<YieldableCommand> yieldableCommands;
        private final TcpServerConnection conn;
        private final int sessionTimeout;
        final Session session;
        final int sessionId;
        private long lastActiveTime;

        SessionInfo(TcpServerConnection conn, Session session, int sessionId, int sessionTimeout) {
            scheduler = ScheduleService.getSchedulerForSession();
            yieldableCommands = new ConcurrentLinkedDeque<>();
            this.conn = conn;
            this.session = session;
            this.sessionId = sessionId;
            this.sessionTimeout = sessionTimeout;
            updateLastActiveTime();
            scheduler.addSessionInfo(this);
        }

        void updateLastActiveTime() {
            lastActiveTime = System.currentTimeMillis();
        }

        public void submitYieldableCommand(int packetId, PreparedSQLStatement stmt,
                PreparedSQLStatement.Yieldable<?> yieldable) {
            YieldableCommand command = new YieldableCommand(packetId, this, stmt, yieldable);
            // 如果当前线程就是当前session的scheduler，可以做一些优化，满足一些条件后可以不用放到队列中直接执行即可。
            if (scheduler == Thread.currentThread()) {
                // 同一个session中的上一个事务还在执行中，不能立刻执行它
                if (session.getStatus() == SessionStatus.TRANSACTION_COMMITTING) {
                    yieldableCommands.add(command);
                } else {
                    // 如果command的优先级最高，立即执行它
                    YieldableCommand bestCommand = scheduler.getNextBestCommand(stmt.getPriority(), true);
                    if (bestCommand == null) {
                        command.execute();
                    } else {
                        yieldableCommands.add(command);
                    }
                }
            } else {
                yieldableCommands.add(command);
                scheduler.wakeUp(); // 及时唤醒
            }
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
            if (lastActiveTime + sessionTimeout < currentTime) {
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
    private YieldableCommand nextBestCommand;

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
    public void addPeriodicTask(AsyncPeriodicTask task) {
        periodicQueue.add(task);
    }

    @Override
    public void removePeriodicTask(AsyncPeriodicTask task) {
        periodicQueue.remove(task);
    }

    @Override
    public void executeNextStatement() {
        int priority = PreparedSQLStatement.MIN_PRIORITY - 1; // 最小优先级减一，保证能取到最小的
        YieldableCommand last = null;
        while (true) {
            YieldableCommand c;
            if (nextBestCommand != null) {
                c = nextBestCommand;
                nextBestCommand = null;
            } else {
                c = getNextBestCommand(priority, true);
            }
            if (c == null) {
                checkSessionTimeout();
                handlePeriodicTasks();
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
                c.si.conn.sendError(c.si.session, c.packetId, e);
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
            YieldableCommand c = getNextBestCommand(priority, false);
            if (c == null) {
                break;
            }

            hasHigherPriorityCommand = true;
            try {
                c.execute();
            } catch (Throwable e) {
                c.si.conn.sendError(c.si.session, c.packetId, e);
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

    private YieldableCommand getNextBestCommand(int priority, boolean checkStatus) {
        if (sessions.isEmpty())
            return null;

        ConcurrentLinkedDeque<YieldableCommand> best = null;

        for (SessionInfo si : sessions) {
            YieldableCommand c = si.yieldableCommands.peek();
            if (c == null)
                continue;

            if (checkStatus) {
                SessionStatus sessionStatus = si.session.getStatus();
                if (sessionStatus == SessionStatus.TRANSACTION_NOT_COMMIT) {
                    Transaction t = si.session.getTransaction();
                    if (t.getStatus() == Transaction.STATUS_WAITING) {
                        try {
                            t.checkTimeout();
                        } catch (Throwable e) {
                            si.yieldableCommands.poll(); // 移除当前命令
                            t.rollback();
                            si.conn.sendError(si.session, c.packetId, e);
                        }
                        continue;
                    }
                } else if (sessionStatus == SessionStatus.TRANSACTION_COMMITTING
                        || sessionStatus == SessionStatus.EXCLUSIVE_MODE || sessionStatus == SessionStatus.WAITING
                        || sessionStatus == SessionStatus.REPLICA_STATEMENT_COMPLETED) {
                    continue;
                }
            }

            if (c.stmt.getPriority() > priority) {
                best = si.yieldableCommands;
                priority = c.stmt.getPriority();
            }
        }

        if (best != null)
            return best.poll();
        else
            return null;
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

    private void handlePeriodicTasks() {
        if (periodicQueue.isEmpty())
            return;
        for (int i = 0, size = periodicQueue.size(); i < size; i++) {
            periodicQueue.get(i).run();
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
    }

    @Override
    public void operationUndo() {
        counter.decrementAndGet();
        wakeUp();
    }

    @Override
    public void operationComplete() {
        counter.decrementAndGet();
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
