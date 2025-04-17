/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.scheduler;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.Map;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.MemoryManager;
import com.lealone.db.async.AsyncTask;
import com.lealone.db.link.LinkableBase;
import com.lealone.db.link.LinkableList;
import com.lealone.db.scheduler.InternalSchedulerBase;
import com.lealone.db.session.InternalSession;
import com.lealone.db.session.ServerSession;
import com.lealone.db.session.Session;
import com.lealone.db.session.SessionInfo;
import com.lealone.net.NetEventLoop;
import com.lealone.net.NetFactory;
import com.lealone.server.AsyncServerManager;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.PreparedSQLStatement.YieldableCommand;
import com.lealone.storage.page.PageOperation;
import com.lealone.storage.page.PageOperation.PageOperationResult;
import com.lealone.transaction.TransactionEngine;

public class GlobalScheduler extends InternalSchedulerBase {

    private static final Logger logger = LoggerFactory.getLogger(GlobalScheduler.class);

    // 预防客户端不断创建新连接试探用户名和密码，试错多次后降低接入新连接的速度
    private final SessionValidator sessionValidator = new SessionValidator();
    private final LinkableList<SessionInitTask> sessionInitTasks = new LinkableList<>();
    private final LinkableList<ServerSessionInfo> sessions = new LinkableList<>();

    // 杂七杂八的任务，数量不多，执行完就删除
    private final LinkableList<LinkableTask> miscTasks = new LinkableList<>();

    private final NetEventLoop netEventLoop;

    private YieldableCommand nextBestCommand;

    public GlobalScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "ScheduleService-" + id, schedulerCount, config);
        netEventLoop = NetFactory.getFactory(config).createNetEventLoop(this, loopInterval);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public long getLoad() {
        return super.getLoad() + sessions.size() + lockedPageOperationTasks.size();
    }

    @Override
    public void run() {
        while (!stopped) {
            runRegisterAccepterTasks();
            runSessionInitTasks();
            runMiscTasks();
            runPageOperationTasks();
            runSessionTasks();
            runPendingTransactions();
            gcCompletedTasks();
            executeNextStatement();
            runPeriodicTasks();
            runEventLoop();
        }
        onStopped();
    }

    @Override
    public void handle(AsyncTask task) {
        LinkableTask ltask = new LinkableTask() {
            @Override
            public void run() {
                task.run();
            }
        };
        miscTasks.add(ltask);
    }

    @Override
    protected void runMiscTasks() {
        if (!miscTasks.isEmpty()) {
            LinkableTask task = miscTasks.getHead();
            while (task != null) {
                // 先取出旧的，避免重复执行
                LinkableTask old = task;
                task = task.next;
                miscTasks.setHead(task);
                miscTasks.decrementSize();
                if (miscTasks.getHead() == null)
                    miscTasks.setTail(null);
                try {
                    old.run();
                } catch (Throwable e) {
                    logger.warn("Failed to run misc task: " + old, e);
                }
            }
            if (miscTasks.getHead() == null)
                miscTasks.setTail(null);
        }
    }

    private void addSessionInfo(ServerSessionInfo si) {
        sessions.add(si);
        // 此时可以初始化了
        si.getSession().init();
    }

    private void removeSessionInfo(ServerSessionInfo si) {
        if (!si.getSession().isClosed())
            sessions.remove(si);
    }

    @Override
    public void addSession(InternalSession session) {
        ServerSession s = (ServerSession) session;
        ServerSessionInfo si = new ServerSessionInfo(this, null, s, -1, -1);
        addSessionInfo(si);
    }

    @Override
    public void removeSession(InternalSession session) {
        if (sessions.isEmpty() || session.isClosed())
            return;
        ServerSessionInfo si = sessions.getHead();
        while (si != null) {
            if (si.getSession() == session) {
                sessions.remove(si);
                break;
            }
            si = si.next;
        }
    }

    private void runSessionTasks() {
        if (sessions.isEmpty())
            return;
        ServerSessionInfo si = sessions.getHead();
        while (si != null) {
            if (!si.isMarkClosed())
                si.runSessionTasks();
            si = si.next;
        }
    }

    private void checkSessionTimeout() {
        if (sessions.isEmpty())
            return;
        long currentTime = System.currentTimeMillis();
        ServerSessionInfo si = sessions.getHead();
        while (si != null) {
            si.checkSessionTimeout(currentTime);
            si = si.next;
        }
    }

    private void addSessionInitTask(SessionInitTask task) {
        sessionInitTasks.add(task);
    }

    private void runSessionInitTasks() {
        if (!sessionInitTasks.isEmpty() && canHandleNextSessionInitTask()) {
            int size = sessionInitTasks.size();
            SessionInitTask task = sessionInitTasks.getHead();
            for (int i = 0; i < size; i++) {
                try {
                    if (!task.run()) {
                        // 继续加到最后，但是不会马上执行
                        // 要copy一下，否则next又指向自己
                        sessionInitTasks.add(task.copy());
                    }
                } catch (Throwable e) {
                    logger.warn("Failed to run session init task: " + task, e);
                }
                task = task.next;
                sessionInitTasks.setHead(task);
                sessionInitTasks.decrementSize();
                if (!canHandleNextSessionInitTask()) {
                    break;
                }
            }
            if (sessionInitTasks.getHead() == null)
                sessionInitTasks.setTail(null);
        }
    }

    @Override
    public void validateSession(boolean isUserAndPasswordCorrect) {
        sessionValidator.validate(isUserAndPasswordCorrect);
    }

    boolean canHandleNextSessionInitTask() {
        return sessionValidator.canHandleNextSessionInitTask();
    }

    private void gc() {
        if (MemoryManager.needFullGc()) {
            ServerSessionInfo si = sessions.getHead();
            while (si != null) {
                si.getSession().clearQueryCache();
                si = si.next;
            }
            TransactionEngine te = TransactionEngine.getDefaultTransactionEngine();
            te.fullGc(getId());
        }
    }

    // --------------------- 实现 SQLStatement 相关的代码 ---------------------

    @Override
    public void executeNextStatement() {
        int priority = PreparedSQLStatement.MIN_PRIORITY - 1; // 最小优先级减一，保证能取到最小的
        YieldableCommand last = null;
        while (true) {
            if (netEventLoop.needWriteImmediately())
                netEventLoop.write();
            gc();
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
            if (c == null) {
                runRegisterAccepterTasks();
                checkSessionTimeout();
                runPageOperationTasks();
                runSessionTasks();
                runPendingTransactions();
                gcCompletedTasks();
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
                    runSessionTasks();
                    runMiscTasks();
                }
                last = c;
            } catch (Throwable e) {
                ServerSessionInfo si = sessions.getHead();
                while (si != null) {
                    if (si.getSessionId() == c.getSessionId()) {
                        si.sendError(c.getPacketId(), e);
                        break;
                    }
                    si = si.next;
                }
            }
        }
    }

    @Override
    public boolean yieldIfNeeded(PreparedSQLStatement current) {
        // 如果有新的session需要创建，那么先接入新的session
        runRegisterAccepterTasks();
        try {
            netEventLoop.getSelector().selectNow();
        } catch (IOException e) {
            logger.warn("Failed to selectNow", e);
        }
        netEventLoop.handleSelectedKeys();
        netEventLoop.write();
        runSessionInitTasks();
        runSessionTasks();
        netEventLoop.write();

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
        ServerSessionInfo si = sessions.getHead();
        while (si != null) {
            // 执行yieldIfNeeded时，不需要检查当前session
            if (currentSession == si.getSession() || si.isMarkClosed()) {
                si = si.next;
                continue;
            }
            YieldableCommand c = si.getYieldableCommand(checkTimeout);
            si = si.next;
            if (c == null)
                continue;
            if (c.getPriority() > priority) {
                best = c;
                priority = c.getPriority();
            }
        }
        return best;
    }

    // --------------------- 实现 PageOperation 相关代码 ---------------------

    protected final LinkableList<LinkablePageOperation> lockedPageOperationTasks = new LinkableList<>();

    protected static class LinkablePageOperation extends LinkableBase<LinkablePageOperation> {
        final PageOperation po;

        public LinkablePageOperation(PageOperation po) {
            this.po = po;
        }
    }

    @Override
    public void handlePageOperation(PageOperation po) {
        lockedPageOperationTasks.add(new LinkablePageOperation(po));
    }

    @Override
    protected void runPageOperationTasks() {
        if (lockedPageOperationTasks.isEmpty())
            return;
        while (lockedPageOperationTasks.getHead() != null) {
            int size = lockedPageOperationTasks.size();
            LinkablePageOperation task = lockedPageOperationTasks.getHead();
            LinkablePageOperation last = null;
            while (task != null) {
                Session old = getCurrentSession();
                setCurrentSession(task.po.getSession());
                try {
                    PageOperationResult result = task.po.run(this);
                    if (result == PageOperationResult.LOCKED) {
                        last = task;
                        task = task.next;
                        continue;
                    } else if (result == PageOperationResult.RETRY) {
                        continue;
                    }
                    task = task.next;
                    if (last == null)
                        lockedPageOperationTasks.setHead(task);
                    else
                        last.next = task;
                } catch (Throwable e) {
                    getLogger().warn("Failed to run page operation: " + task, e);
                } finally {
                    setCurrentSession(old);
                }
                lockedPageOperationTasks.decrementSize();
            }
            if (lockedPageOperationTasks.getHead() == null)
                lockedPageOperationTasks.setTail(null);
            else
                lockedPageOperationTasks.setTail(last);

            // 全都锁住了，没必要再试了
            if (size == lockedPageOperationTasks.size())
                break;
        }
    }

    // --------------------- 实现 Scheduler 接口 ---------------------

    @Override
    public void addSessionInitTask(Object task) {
        addSessionInitTask((SessionInitTask) task);
    }

    @Override
    public void addSessionInfo(SessionInfo si) {
        addSessionInfo((ServerSessionInfo) si);
    }

    @Override
    public void removeSessionInfo(SessionInfo si) {
        removeSessionInfo((ServerSessionInfo) si);
    }

    // --------------------- 注册 Accepter ---------------------

    private void runRegisterAccepterTasks() {
        AsyncServerManager.runRegisterAccepterTasks(this);
    }

    // --------------------- 网络事件循环相关 ---------------------

    @Override
    public NetEventLoop getNetEventLoop() {
        return netEventLoop;
    }

    @Override
    public Selector getSelector() {
        return netEventLoop.getSelector();
    }

    @Override
    public void wakeUp() {
        netEventLoop.wakeUp();
    }

    @Override
    protected void runEventLoop() {
        try {
            netEventLoop.write();
            netEventLoop.select();
            netEventLoop.handleSelectedKeys();
        } catch (Throwable t) {
            getLogger().warn("Failed to runEventLoop", t);
        }
    }

    @Override
    protected void onStopped() {
        super.onStopped();
        netEventLoop.close();
    }
}
