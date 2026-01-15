/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.async.AsyncTask;
import com.lealone.db.session.InternalSession;
import com.lealone.db.session.Session;
import com.lealone.db.session.SessionInfo;
import com.lealone.db.util.Awaiter;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.PreparedSQLStatement.YieldableCommand;
import com.lealone.storage.page.PageOperation;
import com.lealone.storage.page.PageOperation.PageOperationResult;
import com.lealone.transaction.PendingTransaction;

public class EmbeddedScheduler extends InternalSchedulerBase {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedScheduler.class);
    private final Awaiter awaiter = new Awaiter(logger);

    // 杂七杂八的任务，数量不多，执行完就删除
    private final ConcurrentLinkedQueue<AsyncTask> miscTasks = new ConcurrentLinkedQueue<>();
    private YieldableCommand nextBestCommand;

    public EmbeddedScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "EScheduleService-" + id, schedulerCount, config);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public long getLoad() {
        return pageOperationSize.get() + miscTasks.size();
    }

    @Override
    public boolean isEmbedded() {
        return true;
    }

    @Override
    public void handle(AsyncTask task) {
        miscTasks.add(task);
        wakeUp();
    }

    @Override
    protected void runMiscTasks() {
        runMiscTasks(miscTasks);
    }

    @Override
    public void run() {
        while (!stopped) {
            runMiscTasks();
            runSessionTasks();
            runPageOperationTasks();
            runPendingTransactions();
            gcCompletedTasks();
            executeNextStatement();
            runPeriodicTasks();
            doAwait();
        }
        onStopped();
    }

    @Override
    public void wakeUp() {
        awaiter.wakeUp();
    }

    private void doAwait() {
        awaiter.doAwait(loopInterval);
    }

    // --------------------- 跟 PendingTransaction 相关 ---------------------
    // --------------------- 嵌入式场景可能有多个线程在写，所以需要加synchronized ----
    @Override
    public synchronized void runPendingTransactions() {
        super.runPendingTransactions();
    }

    @Override
    public synchronized void addPendingTransaction(PendingTransaction pt) {
        super.addPendingTransaction(pt);
    }

    // --------------------- 实现 PageOperation 相关代码 ---------------------

    // LinkedBlockingQueue测出的性能不如ConcurrentLinkedQueue好
    // 外部线程和调度线程会并发访问这个队列
    private final ConcurrentLinkedQueue<PageOperation> pageOperations = new ConcurrentLinkedQueue<>();
    private final AtomicLong pageOperationSize = new AtomicLong();
    private PageOperation lockedPageOperation;

    @Override
    public void handlePageOperation(PageOperation po) {
        pageOperationSize.incrementAndGet();
        pageOperations.add(po);
        wakeUp();
    }

    @Override
    protected void runPageOperationTasks() {
        PageOperation po;
        // 先执行上一个被锁定的PageOperation，严格保证每个PageOperation的执行顺序
        if (lockedPageOperation != null) {
            po = lockedPageOperation;
            lockedPageOperation = null;
        } else {
            po = pageOperations.poll();
        }
        while (po != null) {
            try {
                PageOperationResult result = po.run(this);
                if (result == PageOperationResult.LOCKED) {
                    lockedPageOperation = po;
                    break;
                } else if (result == PageOperationResult.RETRY) {
                    continue;
                }
            } catch (Throwable e) {
                getLogger().warn("Failed to run page operation: " + po, e);
            }
            pageOperationSize.decrementAndGet();
            po = pageOperations.poll();
        }
    }

    private final CopyOnWriteArrayList<EmbeddedSessionInfo> sessions = new CopyOnWriteArrayList<>();

    private static class EmbeddedSessionInfo implements SessionInfo {

        private final InternalSession session;
        private final Scheduler scheduler;
        private final LinkedBlockingQueue<AsyncTask> tasks = new LinkedBlockingQueue<>();

        public EmbeddedSessionInfo(InternalSession session, Scheduler scheduler) {
            this.session = session;
            this.scheduler = scheduler;
            this.session.setSessionInfo(this);
        }

        @Override
        public Session getSession() {
            return session;
        }

        @Override
        public int getSessionId() {
            return 0;
        }

        @Override
        public void submitTask(AsyncTask task) {
            tasks.add(task);
        }

        void runSessionTasks() {
            if (session.getYieldableCommand() != null) {
                return;
            }
            if (!tasks.isEmpty()) {
                AsyncTask task;
                while ((task = tasks.poll()) != null) {
                    runTask(task);
                    if (session.getYieldableCommand() != null)
                        break;
                }
            }
        }

        private void runTask(AsyncTask task) {
            Session old = scheduler.getCurrentSession();
            scheduler.setCurrentSession(session);
            try {
                task.run();
            } catch (Throwable e) {
                logger.warn(
                        "Failed to run async session task: " + task + ", session id: " + getSessionId(),
                        e);
            } finally {
                scheduler.setCurrentSession(old);
            }
        }
    }

    private void runSessionTasks() {
        if (sessions.isEmpty())
            return;
        for (EmbeddedSessionInfo si : sessions) {
            si.runSessionTasks();
        }
    }

    @Override
    public void addSession(InternalSession session) {
        sessions.add(new EmbeddedSessionInfo(session, this));
        session.init();
    }

    @Override
    public void removeSession(InternalSession session) {
        if (sessions.isEmpty())
            return;
        for (EmbeddedSessionInfo si : sessions) {
            if (si.session == session) {
                sessions.remove(si);
                break;
            }
        }
    }

    @Override
    public <T> AsyncResult<T> await(AsyncCallback<T> ac, long timeoutMillis) {
        executeNextStatement(ac);
        return ac.getAsyncResult();
    }

    // --------------------- 实现 SQLStatement 相关的代码 ---------------------

    @Override
    public void executeNextStatement() {
        executeNextStatement(null);
    }

    private void executeNextStatement(AsyncCallback<?> ac) {
        int priority = PreparedSQLStatement.MIN_PRIORITY - 1; // 最小优先级减一，保证能取到最小的
        YieldableCommand last = null;
        while (true) {
            YieldableCommand c;
            if (nextBestCommand != null) {
                c = nextBestCommand;
                nextBestCommand = null;
            } else {
                c = getNextBestCommand(null, priority, true);
            }
            if (c == null) {
                runSessionTasks();
                c = getNextBestCommand(null, priority, true);
            }
            // 同步执行prepareStatement时
            if (ac != null && ac.getAsyncResult() != null) {
                return;
            }
            if (c == null) {
                runPageOperationTasks();
                runPendingTransactions();
                gcCompletedTasks();
                runMiscTasks();
                runSessionTasks();
                c = getNextBestCommand(null, priority, true);
                if (c == null) {
                    if (ac != null) {
                        doAwait();
                        continue;
                    } else {
                        return;
                    }
                }
            }
            try {
                currentSession = c.getSession();
                c.run();

                if (ac != null && ac.getAsyncResult() != null) {
                    return;
                }
                // 说明没有新的命令了，一直在轮循
                if (last == c) {
                    runPageOperationTasks();
                    runPendingTransactions();
                    runMiscTasks();
                    runSessionTasks();
                }
                last = c;
            } catch (Throwable e) {
                logger.warn("Failed to statement: " + c, e);
            }
        }
    }

    @Override
    public boolean yieldIfNeeded(PreparedSQLStatement current) {
        // 如果有新的session需要创建，那么先接入新的session
        runMiscTasks();
        runSessionTasks();

        // 如果为null说明当前执行的任务优先级很低，比如正在为一个现有的表创建新的索引
        if (current == null) {
            int priority = PreparedSQLStatement.MIN_PRIORITY - 1;
            nextBestCommand = getNextBestCommand(null, priority, false);
            return nextBestCommand != null;
        }

        // 至少有两个session才需要yield
        if (sessions.size() < 2)
            return false;

        // 如果来了更高优化级的命令，那么当前正在执行的语句就让出当前线程，
        // 当前线程转去执行高优先级的命令
        int priority = current.getPriority();
        nextBestCommand = getNextBestCommand(current.getSession(), priority, false);
        if (nextBestCommand != null) {
            current.setPriority(priority + 1);
            return true;
        }
        return false;
    }

    private YieldableCommand getNextBestCommand(Session currentSession, int priority,
            boolean checkTimeout) {
        if (sessions.isEmpty())
            return null;
        YieldableCommand best = null;
        for (EmbeddedSessionInfo si : sessions) {
            // 执行yieldIfNeeded时，不需要检查当前session
            if (currentSession == si) {
                continue;
            }
            YieldableCommand c = si.session.getYieldableCommand(false, null);
            if (c == null)
                continue;
            if (c.getPriority() > priority) {
                best = c;
                priority = c.getPriority();
            }
        }
        return best;
    }
}
