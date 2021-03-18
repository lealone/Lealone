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
import org.lealone.db.async.AsyncPeriodicTask;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.AsyncTaskHandler;
import org.lealone.db.session.ServerSession;
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
            // 同一session中的语句是按顺序一条一条执行的，
            // 如时返回false，说明当前语句执行完成了，切换到下一条；
            // 如果返回true，说明因为某些原因导致主动让出CPU，需要等待获得重新执行的机会。
            if (!yieldable.run()) {
                si.yieldableCommand = null;
            }
        }
    }

    public static class SessionInfo {
        // taskQueue中的命令统一由scheduler调度执行
        private final Scheduler scheduler;
        private final ConcurrentLinkedQueue<AsyncTask> taskQueue;
        private final TcpServerConnection conn;

        private final int sessionTimeout;
        final int sessionId;
        ServerSession session; // 处理完第一个InitPacket任务后才会赋值

        private YieldableCommand yieldableCommand;
        private long lastActiveTime;

        SessionInfo(TcpServerConnection conn, ServerSession session, int sessionId, int sessionTimeout) {
            scheduler = ScheduleService.getSchedulerForSession();
            taskQueue = new ConcurrentLinkedQueue<>();
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

        void submitTask(AsyncTask task) {
            updateLastActiveTime();
            taskQueue.add(task);
            scheduler.wakeUp();
        }

        public void submitYieldableCommand(int packetId, PreparedSQLStatement stmt,
                PreparedSQLStatement.Yieldable<?> yieldable) {
            yieldableCommand = new YieldableCommand(packetId, this, stmt, yieldable);
        }

        void remove() {
            scheduler.removeSessionInfo(this);
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

        void checkTransactionTimeout() {
            Transaction t = session.getTransaction();
            if (t.getStatus() == Transaction.STATUS_WAITING) {
                try {
                    t.checkTimeout();
                } catch (Throwable e) {
                    t.rollback();
                    conn.sendError(session, yieldableCommand.packetId, e);
                    yieldableCommand = null; // 移除当前命令
                }
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
        while (!stop) {
            runQueueTasks(maxPriorityQueue);
            runQueueTasks(normPriorityQueue);
            runQueueTasks(minPriorityQueue);

            runPageOperationTasks();
            runSessionTasks();
            executeNextStatement();
        }
    }

    private void runSessionTasks() {
        if (sessions.isEmpty())
            return;
        for (SessionInfo si : sessions) {
            // 在同一session中，只有前面一条SQL执行完后才可以执行下一条
            // 如果是复制模式，那就可以执行下一个任务(比如异步提交)
            if (si.yieldableCommand == null || (si.session != null && si.session.getReplicationName() != null)) {
                AsyncTask task = si.taskQueue.poll();
                while (task != null) {
                    runTask(task);
                    if (si.yieldableCommand != null)
                        break;
                    task = si.taskQueue.poll();
                }
            }
        }
    }

    private void runTask(AsyncTask task) {
        try {
            task.run();
        } catch (Throwable e) {
            logger.warn("Failed to run async task: " + task, e);
        }
    }

    private void runQueueTasks(ConcurrentLinkedQueue<AsyncTask> queue) {
        AsyncTask task = queue.poll();
        while (task != null) {
            runTask(task);
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
        return sessions.size();
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
                runSessionTasks();
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
                    runSessionTasks();
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
    public boolean yieldIfNeeded(PreparedSQLStatement current) {
        // 如果来了更高优化级的命令，那么当前正在执行的语句就让出当前线程，
        // 当前线程转去执行高优先级的命令
        int priority = current.getPriority();
        nextBestCommand = getNextBestCommand(priority, false);
        if (nextBestCommand != null) {
            current.setPriority(priority + 1);
            return true;
        }
        return false;
    }

    private YieldableCommand getNextBestCommand(int priority, boolean checkTimeout) {
        if (sessions.isEmpty())
            return null;
        YieldableCommand best = null;
        for (SessionInfo si : sessions) {
            YieldableCommand c = si.yieldableCommand;
            if (c == null)
                continue;

            // session处于以下状态时不会被当成候选的对象
            switch (si.session.getStatus()) {
            case WAITING:
                // 复制模式下不主动检查超时
                if (checkTimeout && si.session.getReplicationName() == null) {
                    si.checkTransactionTimeout();
                }
            case TRANSACTION_COMMITTING:
            case EXCLUSIVE_MODE:
            case REPLICA_STATEMENT_COMPLETED:
                continue;
            }

            if (c.stmt.getPriority() > priority) {
                best = c;
                priority = c.stmt.getPriority();
            }
        }
        return best;
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
