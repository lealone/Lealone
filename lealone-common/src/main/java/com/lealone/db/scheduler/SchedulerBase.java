/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.lealone.common.util.MapUtils;
import com.lealone.common.util.ShutdownHookUtils;
import com.lealone.db.DataBufferFactory;
import com.lealone.db.RunMode;
import com.lealone.db.async.AsyncPeriodicTask;
import com.lealone.db.async.AsyncTask;
import com.lealone.db.link.LinkableList;
import com.lealone.db.session.Session;
import com.lealone.server.ProtocolServer;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.page.PageOperation;
import com.lealone.transaction.PendingTransaction;

public abstract class SchedulerBase implements Scheduler {

    protected final int id;
    protected final String name;

    protected final long loopInterval;
    protected boolean started;
    protected boolean stopped;

    protected SchedulerThread thread;
    protected SchedulerFactory schedulerFactory;

    protected final AtomicReferenceArray<Scheduler> waitingSchedulers;
    protected final AtomicBoolean hasWaitingSchedulers = new AtomicBoolean(false);
    protected Session currentSession;

    // 执行一些周期性任务，数量不多，以读为主，所以用LinkableList
    // 用LinkableList是安全的，所有的初始PeriodicTask都在main线程中注册，新的PeriodicTask在当前调度线程中注册
    protected final LinkableList<AsyncPeriodicTask> periodicTasks = new LinkableList<>();

    public SchedulerBase(int id, String name, int schedulerCount, Map<String, String> config) {
        this.id = id;
        this.name = name;
        // 默认100毫秒
        loopInterval = MapUtils.getLong(config, "scheduler_loop_interval", 100);

        waitingSchedulers = new AtomicReferenceArray<>(schedulerCount);

        thread = new SchedulerThread(this);
        thread.setName(name);
        thread.setDaemon(RunMode.isEmbedded(config));
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public long getLoad() {
        return 0;
    }

    @Override
    public SchedulerThread getThread() {
        return thread;
    }

    @Override
    public SchedulerFactory getSchedulerFactory() {
        return schedulerFactory;
    }

    @Override
    public void setSchedulerFactory(SchedulerFactory schedulerFactory) {
        this.schedulerFactory = schedulerFactory;
    }

    @Override
    public synchronized void start() {
        if (started)
            return;
        ShutdownHookUtils.addShutdownHook(getName(), () -> {
            stop();
        });
        thread.start();
        started = true;
        stopped = false;
    }

    @Override
    public synchronized void stop() {
        started = false;
        stopped = true;
        thread = null;
        wakeUp();
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void addSession(Session session) {
    }

    @Override
    public void removeSession(Session session) {
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return DataBufferFactory.getConcurrentFactory();
    }

    @Override
    public Object getNetEventLoop() {
        return null;
    }

    @Override
    public Selector getSelector() {
        return null;
    }

    @Override
    public void registerAccepter(ProtocolServer server, ServerSocketChannel serverChannel) {
    }

    @Override
    public void accept(SelectionKey key) {
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

    // --------------------- 实现 SchedulerListener.Factory 接口 ---------------------

    @Override
    public <R> SchedulerListener<R> createSchedulerListener() {
        return new SchedulerListener<R>() {
            @Override
            public R await() {
                for (;;) {
                    if (result != null || exception != null)
                        break;
                    runMiscTasks();
                    runPageOperationTasks();
                    if (result != null || exception != null)
                        break;
                    runEventLoop();
                }
                if (exception != null)
                    throw exception;
                return result;
            }

            @Override
            public void wakeUp() {
                SchedulerBase.this.wakeUp();
            }
        };
    }

    protected void runEventLoop() {
    }

    protected void runPageOperationTasks() {
    }

    protected void runMiscTasks() {
    }

    protected void runMiscTasks(ConcurrentLinkedQueue<AsyncTask> miscTasks) {
        if (!miscTasks.isEmpty()) {
            AsyncTask task = miscTasks.poll();
            while (task != null) {
                try {
                    task.run();
                } catch (Throwable e) {
                    getLogger().warn("Failed to run misc task: " + task, e);
                }
                task = miscTasks.poll();
            }
        }
    }

    // --------------------- 实现 PageOperation 相关代码 ---------------------

    @Override
    public void handlePageOperation(PageOperation po) {
    }

    @Override
    public void addWaitingScheduler(Scheduler scheduler) {
        int id = scheduler.getId();
        if (id >= 0) {
            waitingSchedulers.set(id, scheduler);
            hasWaitingSchedulers.set(true);
        }
    }

    @Override
    public void wakeUpWaitingSchedulers() {
        if (hasWaitingSchedulers.compareAndSet(true, false)) {
            for (int i = 0, length = waitingSchedulers.length(); i < length; i++) {
                Scheduler scheduler = waitingSchedulers.get(i);
                if (scheduler != null) {
                    scheduler.wakeUp();
                    waitingSchedulers.compareAndSet(i, scheduler, null);
                }
            }
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

    // --------------------- 跟 PendingTransaction 相关 ---------------------

    // 存放还没有给客户端发送响应结果的事务
    protected final LinkableList<PendingTransaction> pendingTransactions = new LinkableList<>();

    // runPendingTransactions和addTransaction已经确保只有一个调度线程执行，所以是单线程安全的
    protected void runPendingTransactions() {
        if (pendingTransactions.isEmpty())
            return;
        PendingTransaction pt = pendingTransactions.getHead();
        while (pt != null && pt.isSynced()) {
            if (!pt.isCompleted()) {
                try {
                    pt.getTransaction().asyncCommitComplete();
                } catch (Throwable e) {
                    getLogger().warn("Failed to run pending transaction: " + pt, e);
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
    public void addPendingTransaction(PendingTransaction pt) {
        pendingTransactions.add(pt);
    }

    @Override
    public PendingTransaction getPendingTransaction() {
        return pendingTransactions.getHead();
    }

    // --------------------- 实现 SQLStatement 相关的代码 ---------------------

    @Override
    public void executeNextStatement() {
    }

    @Override
    public boolean yieldIfNeeded(PreparedSQLStatement current) {
        return false;
    }

    // --------------------- 实现 AsyncTaskHandler 相关的代码 ---------------------
    @Override
    public void addPeriodicTask(AsyncPeriodicTask task) {
        periodicTasks.add(task);
    }

    @Override
    public void removePeriodicTask(AsyncPeriodicTask task) {
        periodicTasks.remove(task);
    }

    protected void runPeriodicTasks() {
        if (periodicTasks.isEmpty())
            return;

        AsyncPeriodicTask task = periodicTasks.getHead();
        while (task != null) {
            try {
                task.run();
            } catch (Throwable e) {
                getLogger().warn("Failed to run periodic task: " + task, e);
            }
            task = task.next;
        }
    }

    // --------------------- 实现 fsync 相关的代码 ---------------------

    protected boolean fsyncDisabled;
    protected FileStorage fsyncingFileStorage;

    @Override
    public boolean isFsyncDisabled() {
        return fsyncDisabled;
    }

    @Override
    public void setFsyncDisabled(boolean fsyncDisabled) {
        this.fsyncDisabled = fsyncDisabled;
    }

    @Override
    public FileStorage getFsyncingFileStorage() {
        return fsyncingFileStorage;
    }

    @Override
    public void setFsyncingFileStorage(FileStorage fsyncingFileStorage) {
        this.fsyncingFileStorage = fsyncingFileStorage;
    }
}
