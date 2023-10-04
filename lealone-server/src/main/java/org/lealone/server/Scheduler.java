/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.MemoryManager;
import org.lealone.db.async.AsyncPeriodicTask;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.AsyncTask;
import org.lealone.db.async.AsyncTaskHandler;
import org.lealone.db.link.LinkableList;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.ServerSession.YieldableCommand;
import org.lealone.db.session.Session;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetEventLoop;
import org.lealone.net.NetFactoryManager;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.storage.page.PageOperationHandlerBase;
import org.lealone.transaction.PendingTransaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionHandler;
import org.lealone.transaction.TransactionListener;

public class Scheduler extends PageOperationHandlerBase //
        implements SQLStatementExecutor, AsyncTaskHandler, //
        PageOperation.ListenerFactory<Object>, //
        TransactionHandler, TransactionListener, NetEventLoop.Accepter {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);

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

    // 注册网络ACCEPT事件，固定是protocol server的个数
    private final RegisterAccepterTask[] registerAccepterTasks;

    private final long loopInterval;
    private final NetEventLoop netEventLoop;

    private boolean end;
    private YieldableCommand nextBestCommand;
    private Session currentSession;

    public Scheduler(int id, int waitingQueueSize, Map<String, String> config) {
        super(id, "ScheduleService-" + id, waitingQueueSize);
        String key = "scheduler_loop_interval";
        // 默认100毫秒
        loopInterval = MapUtils.getLong(config, key, 100);
        netEventLoop = NetFactoryManager.getFactory(config).createNetEventLoop(key, loopInterval, true);
        netEventLoop.setOwner(this);
        netEventLoop.setAccepter(this);

        int protocolServerCount = MapUtils.getInt(config, "protocol_server_count", 1);
        registerAccepterTasks = new RegisterAccepterTask[protocolServerCount];
        for (int i = 0; i < protocolServerCount; i++) {
            registerAccepterTasks[i] = new RegisterAccepterTask();
        }
    }

    @Override
    public Session getCurrentSession() {
        return currentSession;
    }

    @Override
    public void setCurrentSession(Session currentSession) {
        this.currentSession = currentSession;
    }

    @Override
    public boolean isScheduler() {
        return true;
    }

    @Override
    public NetEventLoop getNetEventLoop() {
        return netEventLoop;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public long getLoad() {
        return sessions.size() + super.getLoad();
    }

    public void end() {
        end = true;
        wakeUp();
    }

    @Override
    public void run() {
        while (!end) {
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

    void validateSession(boolean isUserAndPasswordCorrect) {
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
            te.fullGc(SchedulerFactory.getSchedulerCount(), getHandlerId());
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

    private AtomicInteger syncCounter;
    private RuntimeException syncException;
    private boolean needWakeUp = true;

    @Override
    public void setNeedWakeUp(boolean needWakeUp) {
        this.needWakeUp = needWakeUp;
    }

    @Override
    public int getListenerId() {
        return getHandlerId();
    }

    @Override
    public void beforeOperation() {
        syncException = null;
        syncCounter = new AtomicInteger(1);
    }

    @Override
    public void operationUndo() {
        syncCounter.decrementAndGet();
        if (needWakeUp)
            wakeUp();
    }

    @Override
    public void operationComplete() {
        syncCounter.decrementAndGet();
        if (needWakeUp)
            wakeUp();
    }

    @Override
    public void setException(RuntimeException e) {
        syncException = e;
    }

    @Override
    public RuntimeException getException() {
        return syncException;
    }

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

    // --------------------- 实现 PageOperation.ListenerFactory 接口 ---------------------

    @Override
    public PageOperation.Listener<Object> createListener() {
        return new PageOperation.Listener<Object>() {

            private Object result;

            @Override
            public void startListen() {
                beforeOperation();
            }

            @Override
            public void handle(AsyncResult<Object> ar) {
                if (ar.isSucceeded()) {
                    result = ar.getResult();
                } else {
                    setException(ar.getCause());
                }
                operationComplete();
            }

            @Override
            public Object await() {
                Scheduler.this.await();
                return result;
            }
        };
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

    private class RegisterAccepterTask {

        private AsyncServer<?> asyncServer;
        private ServerSocketChannel serverChannel;
        private boolean needRegisterAccepter;

        private void run() {
            if (!needRegisterAccepter)
                return;
            try {
                serverChannel.register(netEventLoop.getSelector(), SelectionKey.OP_ACCEPT, this);
            } catch (ClosedChannelException e) {
                logger.warn("Failed to register server channel: " + serverChannel);
            }
            needRegisterAccepter = false;
        }
    }

    public void registerAccepter(AsyncServer<?> asyncServer, ServerSocketChannel serverChannel) {
        RegisterAccepterTask task = registerAccepterTasks[asyncServer.getServerId()];
        task.asyncServer = asyncServer;
        task.serverChannel = serverChannel;
        task.needRegisterAccepter = true;
        wakeUp();
    }

    private void runRegisterAccepterTasks() {
        for (int i = 0; i < registerAccepterTasks.length; i++) {
            registerAccepterTasks[i].run();
        }
    }

    @Override
    public void accept(SelectionKey key) {
        RegisterAccepterTask task = (RegisterAccepterTask) key.attachment();
        task.asyncServer.getProtocolServer().accept(this);
        if (task.asyncServer.isRoundRobinAcceptEnabled()) {
            Scheduler scheduler = SchedulerFactory.getScheduler();
            // 如果下一个负责处理网络accept事件的调度器又是当前调度器，那么不需要做什么
            if (scheduler != this) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_ACCEPT);
                scheduler.registerAccepter(task.asyncServer, task.serverChannel);
                task.asyncServer = null;
                task.serverChannel = null;
            }
        }
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
}
