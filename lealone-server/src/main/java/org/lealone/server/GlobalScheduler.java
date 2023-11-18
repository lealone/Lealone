/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.MemoryManager;
import org.lealone.db.async.AsyncPeriodicTask;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.link.LinkableBase;
import org.lealone.db.link.LinkableList;
import org.lealone.db.scheduler.ISessionInfo;
import org.lealone.db.scheduler.ISessionInitTask;
import org.lealone.db.scheduler.SchedulerBase;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.ServerSession.YieldableCommand;
import org.lealone.db.session.Session;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetEventLoop;
import org.lealone.net.NetFactoryManager;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperation.PageOperationResult;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.transaction.PendingTransaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionListener;

public class GlobalScheduler extends SchedulerBase implements NetEventLoop.Accepter {

    private static final Logger logger = LoggerFactory.getLogger(GlobalScheduler.class);

    // 预防客户端不断创建新连接试探用户名和密码，试错多次后降低接入新连接的速度
    private final SessionValidator sessionValidator = new SessionValidator();
    private final LinkableList<SessionInitTask> sessionInitTasks = new LinkableList<>();
    private final LinkableList<SessionInfo> sessions = new LinkableList<>();

    // 执行一些周期性任务，数量不多，以读为主，所以用LinkableList
    // 用LinkableList是安全的，所有的初始PeriodicTask都在main线程中注册，新的PeriodicTask在当前调度线程中注册
    private final LinkableList<AsyncPeriodicTask> periodicTasks = new LinkableList<>();

    // 存放还没有给客户端发送响应结果的事务
    private final LinkableList<PendingTransaction> pendingTransactions = new LinkableList<>();

    // 杂七杂八的任务，数量不多，执行完就删除
    private final LinkableList<LinkableTask> miscTasks = new LinkableList<>();

    private final NetEventLoop netEventLoop;

    private YieldableCommand nextBestCommand;

    // 用于实现 PageOperationHandler 接口
    private final LinkableList<LinkablePageOperation> lockedPageOperationTasks = new LinkableList<>();

    public GlobalScheduler(int id, int schedulerCount, Map<String, String> config) {
        super(id, "ScheduleService-" + id, schedulerCount, config);
        netEventLoop = NetFactoryManager.getFactory(config).createNetEventLoop(loopInterval, true);
        netEventLoop.setOwner(this);
        netEventLoop.setScheduler(this);
        netEventLoop.setAccepter(this);
    }

    @Override
    public NetEventLoop getNetEventLoop() {
        return netEventLoop;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public long getLoad() {
        return sessions.size() + lockedPageOperationTasks.size();
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
            executeNextStatement();
            runEventLoop();
        }
        netEventLoop.close();
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

    private void runMiscTasks() {
        if (!miscTasks.isEmpty()) {
            LinkableTask task = miscTasks.getHead();
            while (task != null) {
                try {
                    task.run();
                } catch (Throwable e) {
                    logger.warn("Failed to run misc task: " + task, e);
                }
                task = task.next;
                miscTasks.setHead(task);
                miscTasks.decrementSize();
            }
            if (miscTasks.getHead() == null)
                miscTasks.setTail(null);
        }
    }

    public void addSessionInfo(SessionInfo si) {
        sessions.add(si);
    }

    void removeSessionInfo(SessionInfo si) {
        sessions.remove(si);
    }

    private void runSessionTasks() {
        if (sessions.isEmpty())
            return;
        SessionInfo si = sessions.getHead();
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
        SessionInfo si = sessions.getHead();
        while (si != null) {
            si.checkSessionTimeout(currentTime);
            si = si.next;
        }
    }

    void addSessionInitTask(SessionInitTask task) {
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

    @Override
    public void addPeriodicTask(AsyncPeriodicTask task) {
        periodicTasks.add(task);
    }

    @Override
    public void removePeriodicTask(AsyncPeriodicTask task) {
        periodicTasks.remove(task);
    }

    private void runPeriodicTasks() {
        if (periodicTasks.isEmpty())
            return;

        AsyncPeriodicTask task = periodicTasks.getHead();
        while (task != null) {
            try {
                task.run();
            } catch (Throwable e) {
                logger.warn("Failed to run periodic task: " + task, e);
            }
            task = task.next;
        }
    }

    private void gc() {
        if (MemoryManager.needFullGc()) {
            SessionInfo si = sessions.getHead();
            while (si != null) {
                si.getSession().clearQueryCache();
                si = si.next;
            }
            TransactionEngine te = TransactionEngine.getDefaultTransactionEngine();
            te.fullGc(schedulerFactory.getSchedulerCount(), getHandlerId());
        }
    }

    @Override
    public void executeNextStatement() {
        int priority = PreparedSQLStatement.MIN_PRIORITY - 1; // 最小优先级减一，保证能取到最小的
        YieldableCommand last = null;
        while (true) {
            if (netEventLoop.isQueueLarge())
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
                runPeriodicTasks();
                runPageOperationTasks();
                runSessionTasks();
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
                    runSessionTasks();
                    runMiscTasks();
                }
                last = c;
            } catch (Throwable e) {
                SessionInfo si = sessions.getHead();
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

    // --------------------- 实现 TransactionListener 接口，用同步方式执行 ---------------------

    @Override
    public void await() {
        for (;;) {
            if (syncCounter.get() < 1)
                break;
            runMiscTasks();
            runPageOperationTasks();
            if (syncCounter.get() < 1)
                break;
            runEventLoop();
        }
        needWakeUp = true;
        if (syncException != null)
            throw syncException;
    }

    @Override
    public Object addSession(Session session, Object parentSessionInfo) {
        SessionInfo parent = (SessionInfo) parentSessionInfo;
        SessionInfo si = parent.copy((ServerSession) session);
        ((ServerSession) session).setSessionInfo(si);
        addSessionInfo(si);
        return si;
    }

    @Override
    public void removeSession(Object sessionInfo) {
        SessionInfo si = (SessionInfo) sessionInfo;
        // 不删除root session
        if (!si.getSession().isRoot())
            removeSessionInfo(si);
    }

    // --------------------- 网络事件循环 ---------------------

    @Override
    public void wakeUp() {
        netEventLoop.wakeup();
    }

    private void runEventLoop() {
        try {
            netEventLoop.write();
            netEventLoop.select();
            netEventLoop.handleSelectedKeys();
        } catch (Throwable t) {
            logger.warn("Failed to runEventLoop", t);
        }
    }

    // --------------------- 注册 Accepter 和新的 AsyncConnection ---------------------

    public void register(AsyncConnection conn) {
        conn.getWritableChannel().setEventLoop(netEventLoop); // 替换掉原来的
        netEventLoop.register(conn);
    }

    public void registerAccepter(AsyncServer<?> asyncServer, ServerSocketChannel serverChannel) {
        AsyncServerManager.registerAccepter(asyncServer, serverChannel, this);
        wakeUp();
    }

    private void runRegisterAccepterTasks() {
        AsyncServerManager.runRegisterAccepterTasks(this);
    }

    @Override
    public void accept(SelectionKey key) {
        AsyncServerManager.accept(key, this);
    }

    // --------------------- 实现 TransactionHandler 接口 ---------------------

    // runPendingTransactions和addTransaction已经确保只有一个调度线程执行，所以是单线程安全的
    private void runPendingTransactions() {
        if (pendingTransactions.isEmpty())
            return;
        PendingTransaction pt = pendingTransactions.getHead();
        while (pt != null && pt.isSynced()) {
            if (!pt.isCompleted()) {
                try {
                    pt.getTransaction().asyncCommitComplete();
                } catch (Throwable e) {
                    if (logger != null)
                        logger.warn("Failed to run pending transaction: " + pt, e);
                }
            }
            pt = pt.getNext();
            pendingTransactions.decrementSize();
            pendingTransactions.setHead(pt);
        }
        if (pendingTransactions.getHead() == null)
            pendingTransactions.setTail(null);
    }

    @Override
    public void addTransaction(PendingTransaction pt) {
        pendingTransactions.add(pt);
    }

    @Override
    public PendingTransaction getTransaction() {
        return pendingTransactions.getHead();
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return netEventLoop.getDataBufferFactory();
    }

    @Override
    public void addWaitingTransactionListener(TransactionListener listener) {
        addWaitingHandler((PageOperationHandler) listener);
    }

    @Override
    public void wakeUpWaitingTransactionListeners() {
        wakeUpWaitingHandlers();
    }

    // --------------------- 实现 Scheduler 接口 ---------------------

    @Override
    public void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel) {
        AsyncServerManager.registerAccepter((AsyncServer<?>) server, serverChannel, this);
        wakeUp();
    }

    @Override
    public Selector getSelector() {
        return getNetEventLoop().getSelector();
    }

    @Override
    public void register(Object conn) {
        register((AsyncConnection) conn);
    }

    @Override
    public void addSessionInitTask(ISessionInitTask task) {
        addSessionInitTask((SessionInitTask) task);
    }

    @Override
    public void addSessionInfo(ISessionInfo si) {
        addSessionInfo((SessionInfo) si);
    }

    @Override
    public void removeSessionInfo(ISessionInfo si) {
        removeSessionInfo((SessionInfo) si);
    }

    // --------------------- 实现 PageOperationHandler 接口 ---------------------

    private static class LinkablePageOperation extends LinkableBase<LinkablePageOperation> {
        final PageOperation po;

        public LinkablePageOperation(PageOperation po) {
            this.po = po;
        }
    }

    @Override
    public void handlePageOperation(PageOperation po) {
        lockedPageOperationTasks.add(new LinkablePageOperation(po));
    }

    private void runPageOperationTasks() {
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

    // --------------------- 创建所有的调度器 ---------------------

    public static GlobalScheduler[] createSchedulers(Map<String, String> config) {
        int schedulerCount = MapUtils.getSchedulerCount(config);
        GlobalScheduler[] schedulers = new GlobalScheduler[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            schedulers[i] = new GlobalScheduler(i, schedulerCount, config);
        }
        return schedulers;
    }
}
