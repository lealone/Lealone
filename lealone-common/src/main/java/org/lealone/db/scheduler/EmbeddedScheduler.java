/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.Awaiter;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.PendingTaskHandler;
import org.lealone.db.session.Session;
import org.lealone.server.ProtocolServer;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.PreparedSQLStatement.YieldableCommand;

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
        return super.getLoad() + miscTasks.size();
    }

    @Override
    public void wakeUp() {
        awaiter.wakeUp();
    }

    @Override
    public void run() {
        while (!stopped) {
            runMiscTasks();
            runPageOperationTasks();
            runPendingTransactions();
            runPendingTasks();
            executeNextStatement();
            doAwait();
        }
    }

    private void doAwait() {
        awaiter.doAwait(loopInterval);
    }

    @Override
    protected void runMiscTasks() {
        if (!miscTasks.isEmpty()) {
            AsyncTask task = miscTasks.poll();
            while (task != null) {
                try {
                    task.run();
                } catch (Throwable e) {
                    logger.warn("Failed to run misc task: " + task, e);
                }
                task = miscTasks.poll();
            }
        }
    }

    @Override
    public boolean yieldIfNeeded(PreparedSQLStatement current) {
        return false;
    }

    @Override
    public void handle(AsyncTask task) {
        miscTasks.add(task);
    }

    @Override
    public void operationUndo() {
    }

    @Override
    public void operationComplete() {
    }

    @Override
    public Selector getSelector() {
        return null;
    }

    @Override
    public void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel) {
    }

    @Override
    public void register(Object conn) {
    }

    @Override
    public void addSessionInitTask(Object task) {
    }

    @Override
    public void addSessionInfo(Object si) {
    }

    @Override
    public void removeSessionInfo(Object si) {
    }

    @Override
    public void validateSession(boolean isUserAndPasswordCorrect) {
    }

    @Override
    public void await() {
        for (;;) {
            if (syncCounter.get() < 1)
                break;
            runPageOperationTasks();
            runPendingTransactions();
            if (syncCounter.get() < 1)
                break;
        }
        needWakeUp = true;
        if (syncException != null)
            throw syncException;
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
                c = getNextBestCommand(null, priority, true);
            }
            if (c == null) {
                runMiscTasks();
                c = getNextBestCommand(null, priority, true);
            }
            if (c == null) {
                runPageOperationTasks();
                runPendingTransactions();
                runPendingTasks();
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
                    runPendingTasks();
                    runMiscTasks();
                }
                last = c;
            } catch (Throwable e) {
                logger.warn("Failed to statement: " + c, e);
            }
        }
    }

    private YieldableCommand getNextBestCommand(Session currentSession, int priority,
            boolean checkTimeout) {
        if (pendingTaskHandlers.isEmpty())
            return null;
        YieldableCommand best = null;
        PendingTaskHandler pi = pendingTaskHandlers.getHead();
        while (pi != null) {
            // 执行yieldIfNeeded时，不需要检查当前session
            if (currentSession == pi) {
                pi = pi.getNext();
                continue;
            }
            YieldableCommand c = pi.getYieldableCommand(false, null);
            pi = pi.getNext();
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
