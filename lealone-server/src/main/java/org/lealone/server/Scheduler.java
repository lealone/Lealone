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
import org.lealone.db.session.ServerSession.YieldableCommand;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.transaction.Transaction;

public class Scheduler extends Thread
        implements SQLStatementExecutor, PageOperationHandler, AsyncTaskHandler, Transaction.Listener {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);

    private final CopyOnWriteArrayList<SessionInfo> sessions = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<PageOperation> pageOperationQueue = new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<AsyncTask> minPriorityQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<AsyncTask> normPriorityQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<AsyncTask> maxPriorityQueue = new ConcurrentLinkedQueue<>();

    // 这个只增不删所以用CopyOnWriteArrayList
    private final CopyOnWriteArrayList<AsyncTask> periodicQueue = new CopyOnWriteArrayList<>();

    private final Semaphore haveWork = new Semaphore(1);
    private final long loopInterval;
    private volatile boolean end;
    private volatile boolean waiting;
    private YieldableCommand nextBestCommand;

    public Scheduler(int id, Map<String, String> config) {
        super(ScheduleService.class.getSimpleName() + "-" + id);
        setDaemon(true);
        // 默认100毫秒
        loopInterval = DateTimeUtils.getLoopInterval(config, "scheduler_loop_interval", 100);
    }

    void addSessionInfo(SessionInfo si) {
        sessions.add(si);
    }

    void removeSessionInfo(SessionInfo si) {
        sessions.remove(si);
    }

    @Override
    public void run() {
        while (!end) {
            runQueueTasks(maxPriorityQueue);
            runQueueTasks(normPriorityQueue);
            runQueueTasks(minPriorityQueue);

            runPageOperationTasks();
            runSessionTasks();
            executeNextStatement();
        }
    }

    private void runQueueTasks(ConcurrentLinkedQueue<AsyncTask> queue) {
        AsyncTask task = queue.poll();
        while (task != null) {
            try {
                task.run();
            } catch (Throwable e) {
                logger.warn("Failed to run async queue task: " + task, e);
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

    private void runSessionTasks() {
        if (sessions.isEmpty())
            return;
        for (SessionInfo si : sessions) {
            si.runSessionTasks();
        }
    }

    void end() {
        end = true;
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
                    doAwait();
                    break;
                }
            }
            try {
                c.run();
                // 说明没有新的命令了，一直在轮循
                if (last == c) {
                    runPageOperationTasks();
                    runSessionTasks();
                    runQueueTasks(maxPriorityQueue);
                    runQueueTasks(normPriorityQueue);
                }
                last = c;
            } catch (Throwable e) {
                for (SessionInfo si : sessions) {
                    if (si.getSessionId() == c.getSessionId()) {
                        si.sendError(c.getPacketId(), e);
                        break;
                    }
                }
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
            YieldableCommand c = si.getYieldableCommand(checkTimeout);
            if (c == null)
                continue;
            if (c.getPriority() > priority) {
                best = c;
                priority = c.getPriority();
            }
        }
        return best;
    }

    @Override
    public void wakeUp() {
        if (waiting)
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
            AsyncTask task = periodicQueue.get(i);
            try {
                task.run();
            } catch (Throwable e) {
                logger.warn("Failed to run periodic task: " + task + ", task index: " + i, e);
            }
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
            doAwait();
        }
        if (e != null)
            throw e;
    }

    private void doAwait() {
        waiting = true;
        try {
            haveWork.tryAcquire(loopInterval, TimeUnit.MILLISECONDS);
            haveWork.drainPermits();
        } catch (InterruptedException e) {
            handleInterruptedException(e);
        } finally {
            waiting = false;
        }
    }
}
