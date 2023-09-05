/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.lealone.db.session.ServerSession.YieldableCommand;
import org.lealone.db.session.Session;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetEventLoop;
import org.lealone.net.NetFactoryManager;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperationHandlerBase;
import org.lealone.transaction.TransactionListener;

public class Scheduler extends PageOperationHandlerBase implements Runnable, SQLStatementExecutor,
        AsyncTaskHandler, TransactionListener, PageOperation.ListenerFactory<Object> {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);

    private final CopyOnWriteArrayList<SessionInfo> sessions = new CopyOnWriteArrayList<>();

    private final ConcurrentLinkedQueue<AsyncTask> minPriorityQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<AsyncTask> normPriorityQueue = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<AsyncTask> maxPriorityQueue = new ConcurrentLinkedQueue<>();

    private final ConcurrentLinkedQueue<SessionInitTask> sessionInitTasks = new ConcurrentLinkedQueue<>();
    private final SessionValidator sessionValidator = new SessionValidator();

    // 这个只增不删所以用CopyOnWriteArrayList
    private final CopyOnWriteArrayList<AsyncTask> periodicQueue = new CopyOnWriteArrayList<>();

    private final long loopInterval;
    private volatile boolean end;
    private YieldableCommand nextBestCommand;
    private NetEventLoop netEventLoop;
    private Session currentSession;

    public Scheduler(int id, int waitingQueueSize, Map<String, String> config) {
        super(id, "ScheduleService-" + id, waitingQueueSize);
        String key = "scheduler_loop_interval";// 默认100毫秒
        loopInterval = MapUtils.getLong(config, key, 100);
        netEventLoop = NetFactoryManager.getFactory(config).createNetEventLoop(key, loopInterval);
        netEventLoop.setOwner(this);
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

    public NetEventLoop getNetEventLoop() {
        return netEventLoop;
    }

    @Override
    protected Logger getLogger() {
        return logger;
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
            runSessionInitTasks();
            runQueueTasks(maxPriorityQueue);
            runQueueTasks(normPriorityQueue);
            runQueueTasks(minPriorityQueue);

            runPageOperationTasks();
            runSessionTasks();
            executeNextStatement();
            runEventLoop();
        }
        netEventLoop.close();
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

    private void runSessionTasks() {
        if (sessions.isEmpty())
            return;
        // 不适合使用普通for循环，sessions会随时新增或删除元素，
        // 只能每次创建一个迭代器包含元素数组的快照
        for (SessionInfo si : sessions) {
            if (si.isMarkClosed())
                continue;
            si.runSessionTasks();
        }
    }

    private void runSessionInitTasks() {
        if (canHandleNextSessionInitTask()) {
            int size = sessionInitTasks.size();
            for (int i = 0; i < size; i++) {
                SessionInitTask task = sessionInitTasks.poll();
                try {
                    if (!task.run()) {
                        sessionInitTasks.add(task); // 继续加到最后，但是不会马上执行
                    }
                } catch (Throwable e) {
                    logger.warn("Failed to run session init task: " + task, e);
                }
                if (!canHandleNextSessionInitTask()) {
                    break;
                }
            }
        }
    }

    public void validateSession(boolean isUserAndPasswordCorrect) {
        sessionValidator.validate(isUserAndPasswordCorrect);
    }

    void end() {
        end = true;
        wakeUp();
    }

    @Override
    public long getLoad() {
        return sessions.size() + super.getLoad();
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

    boolean canHandleNextSessionInitTask() {
        return sessionValidator.canHandleNextSessionInitTask();
    }

    public void addSessionInitTask(SessionInitTask task) {
        sessionInitTasks.add(task);
    }

    @Override
    public void addPeriodicTask(AsyncPeriodicTask task) {
        periodicQueue.add(task);
    }

    @Override
    public void removePeriodicTask(AsyncPeriodicTask task) {
        periodicQueue.remove(task);
    }

    private void gc() {
        if (sessions.isEmpty())
            return;
        if (MemoryManager.getGlobalMemoryManager().needGc()) {
            for (SessionInfo si : sessions) {
                si.getSession().clearQueryCache();
            }
        }
    }

    @Override
    public void executeNextStatement() {
        int priority = PreparedSQLStatement.MIN_PRIORITY - 1; // 最小优先级减一，保证能取到最小的
        YieldableCommand last = null;
        while (true) {
            gc();
            YieldableCommand c;
            if (nextBestCommand != null) {
                c = nextBestCommand;
                nextBestCommand = null;
            } else {
                c = getNextBestCommand(null, priority, true);
            }
            if (c == null) {
                checkSessionTimeout();
                handlePeriodicTasks();
                runPageOperationTasks();
                runSessionTasks();
                runQueueTasks(maxPriorityQueue);
                runQueueTasks(normPriorityQueue);
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
        try {
            runQueueTasks(maxPriorityQueue);
            runQueueTasks(normPriorityQueue);
            runQueueTasks(minPriorityQueue);
            try {
                netEventLoop.getSelector().selectNow();
            } catch (IOException e) {
                logger.warn("Failed to selectNow", e);
            }
            handleSelectedKeys();
            netEventLoop.write();
            runSessionInitTasks();
            runSessionTasks();
            netEventLoop.write();
        } catch (Exception e) {
            logger.warn("Failed to yieldIfNeeded", e);
            return false;
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
        for (SessionInfo si : sessions) {
            if (si.isMarkClosed())
                continue;
            // 执行yieldIfNeeded时，不需要检查当前session
            if (currentSession == si.getSession())
                continue;
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
        netEventLoop.wakeup();
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
            runEventLoop();
        }
        if (e != null)
            throw e;
    }

    private void runEventLoop() {
        try {
            netEventLoop.write();
            netEventLoop.select();
            handleSelectedKeys();
        } catch (Throwable t) {
            logger.warn("Failed to runEventLoop", t);
        }
    }

    private void handleSelectedKeys() {
        Set<SelectionKey> keys = netEventLoop.getSelector().selectedKeys();
        if (!keys.isEmpty()) {
            try {
                for (SelectionKey key : keys) {
                    if (key.isValid()) {
                        int readyOps = key.readyOps();
                        if ((readyOps & SelectionKey.OP_READ) != 0) {
                            netEventLoop.read(key);
                        } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                            netEventLoop.write(key);
                        } else {
                            key.cancel();
                        }
                    } else {
                        key.cancel();
                    }
                }
            } finally {
                keys.clear();
            }
        }
    }

    public void register(AsyncConnection conn) {
        conn.getWritableChannel().setEventLoop(netEventLoop); // 替换掉原来的
        // 如果Scheduler线程在执行select，
        // 在jdk1.8中不能直接在另一个线程中注册读写操作，否则会阻塞这个线程
        // jdk16不存在这个问题
        handle(() -> {
            netEventLoop.register(conn);
        });
    }

    @Override
    public PageOperation.Listener<Object> createListener() {
        return new PageOperation.Listener<Object>() {

            private volatile Object result;

            @Override
            public void startListen() {
                beforeOperation();
            }

            @Override
            public void handle(AsyncResult<Object> ar) {
                if (ar.isSucceeded()) {
                    result = ar.getResult();
                } else {
                    setException(new RuntimeException(ar.getCause()));
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

    public DataBufferFactory getDataBufferFactory() {
        return DataBufferFactory.getConcurrentFactory();
    }
}
