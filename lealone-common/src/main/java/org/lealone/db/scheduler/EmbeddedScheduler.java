/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.Awaiter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.link.LinkableBase;
import org.lealone.db.link.LinkableList;
import org.lealone.db.session.Session;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.PreparedSQLStatement.YieldableCommand;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperation.PageOperationResult;

public class EmbeddedScheduler extends SchedulerBase {

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
            runPageOperationTasks();
            runPendingTransactions();
            // runPendingTasks();
            executeNextStatement();
            doAwait();
        }
    }

    @Override
    public void wakeUp() {
        awaiter.wakeUp();
    }

    private void doAwait() {
        awaiter.doAwait(loopInterval);
    }

    // --------------------- 实现 PageOperationHandler 接口 ---------------------

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

    protected final LinkableList<SessionInfo> sessions = new LinkableList<>();

    protected static class SessionInfo extends LinkableBase<SessionInfo> {
        final Session session;

        public SessionInfo(Session session) {
            this.session = session;
        }
    }

    @Override
    public void addSession(Session session, int databaseId) {
        sessions.add(new SessionInfo(session));
        session.init();
    }

    @Override
    public void removeSession(Session session) {
        if (sessions.isEmpty())
            return;
        SessionInfo si = sessions.getHead();
        while (si != null) {
            if (si.session == session) {
                sessions.remove(si);
                break;
            }
            si = si.next;
        }
    }

    // --------------------- 实现 SQLStatementExecutor 接口 ---------------------

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
                c = getNextBestCommand(null, priority, true);
            }
            if (c == null) {
                runMiscTasks();
                c = getNextBestCommand(null, priority, true);
            }
            if (c == null) {
                runPageOperationTasks();
                runPendingTransactions();
                runMiscTasks();
                c = getNextBestCommand(null, priority, true);
                if (c == null) {
                    break;
                }
            }
            try {
                currentSession = c.getSession();
                c.run();
                // 说明没有新的命令了，一直在轮循
                if (last == c) {
                    runPageOperationTasks();
                    runPendingTransactions();
                    runMiscTasks();
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
        SessionInfo si = sessions.getHead();
        while (si != null) {
            // 执行yieldIfNeeded时，不需要检查当前session
            if (currentSession == si) {
                si = si.getNext();
                continue;
            }
            YieldableCommand c = si.session.getYieldableCommand(false, null);
            si = si.getNext();
            if (c == null)
                continue;
            if (c.getPriority() > priority) {
                best = c;
                priority = c.getPriority();
            }
        }
        return best;
    }

    public static Scheduler getScheduler(ConnectionInfo ci) {
        return SchedulerFactoryBase.getScheduler(EmbeddedScheduler.class.getName(), ci);
    }
}
