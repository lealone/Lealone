/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.lealone.db.link.LinkableBase;
import com.lealone.db.link.LinkableList;
import com.lealone.db.session.InternalSession;
import com.lealone.db.session.Session;
import com.lealone.db.session.SessionInfo;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.page.PageOperation;
import com.lealone.transaction.PendingTransaction;

public abstract class InternalSchedulerBase extends SchedulerBase implements InternalScheduler {

    protected final AtomicReferenceArray<Scheduler> waitingSchedulers;
    protected final AtomicBoolean hasWaitingSchedulers = new AtomicBoolean(false);

    public InternalSchedulerBase(int id, String name, int schedulerCount, Map<String, String> config) {
        super(id, name, schedulerCount, config);
        waitingSchedulers = new AtomicReferenceArray<>(schedulerCount);
    }

    @Override
    public InternalSession getCurrentSession() {
        return (InternalSession) currentSession;
    }

    @Override
    public void addSession(InternalSession session) {
    }

    @Override
    public void removeSession(InternalSession session) {
    }

    @Override
    public void addSessionInitTask(Object task) {
    }

    @Override
    public void addSessionInfo(SessionInfo si) {
    }

    @Override
    public void removeSessionInfo(SessionInfo si) {
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
                InternalSchedulerBase.this.wakeUp();
            }
        };
    }

    protected void runPageOperationTasks() {
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
    public void wakeUpWaitingSchedulers(boolean reset) {
        if (reset) {
            wakeUpWaitingSchedulers();
        } else if (hasWaitingSchedulers.get()) {
            for (int i = 0, length = waitingSchedulers.length(); i < length; i++) {
                Scheduler scheduler = waitingSchedulers.get(i);
                if (scheduler != null) {
                    scheduler.wakeUp();
                }
            }
        }
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
        if (pendingTransactions.getHead() == null) {
            pendingTransactions.setTail(null);
            if (logBuffer != null && logBuffer.position() > 0)
                logBuffer.clear();
        }
    }

    @Override
    public void addPendingTransaction(PendingTransaction pt) {
        pendingTransactions.add(pt);
    }

    @Override
    public PendingTransaction getPendingTransaction() {
        return pendingTransactions.getHead();
    }

    // --------------------- 跟 SchedulerTaskManager 相关 ---------------------

    protected final LinkableList<SchedulerTaskManagerWrapper> taskManagers = new LinkableList<>();

    // SchedulerTaskManager需要传递给多个Scheduler，需要包装成SchedulerTaskManagerWrapper
    // 如果SchedulerTaskManager直接继承自LinkableBase，多个Scheduler修改它的next会有并发问题
    protected static class SchedulerTaskManagerWrapper extends LinkableBase<SchedulerTaskManagerWrapper>
            implements SchedulerTaskManager {

        protected SchedulerTaskManager schedulerTaskManager;

        public SchedulerTaskManagerWrapper(SchedulerTaskManager schedulerTaskManager) {
            this.schedulerTaskManager = schedulerTaskManager;
        }

        @Override
        public boolean gcCompletedTasks(InternalScheduler scheduler) {
            return schedulerTaskManager.gcCompletedTasks(scheduler);
        }
    }

    protected void gcCompletedTasks() {
        if (taskManagers.isEmpty())
            return;
        while (taskManagers.getHead() != null) {
            int size = taskManagers.size();
            SchedulerTaskManagerWrapper task = taskManagers.getHead();
            SchedulerTaskManagerWrapper last = null;
            while (task != null) {
                try {
                    if (!task.gcCompletedTasks(this)) {
                        last = task;
                        task = task.next;
                        continue;
                    }
                    task = task.next;
                    if (last == null)
                        taskManagers.setHead(task);
                    else
                        last.next = task;
                } catch (Throwable e) {
                    getLogger().warn("Failed to run task: " + task, e);
                }
                taskManagers.decrementSize();
            }
            if (taskManagers.getHead() == null)
                taskManagers.setTail(null);
            else
                taskManagers.setTail(last);
            if (size == taskManagers.size())
                break;
        }
    }

    @Override
    public void addTaskManager(SchedulerTaskManager taskManager) {
        taskManagers.add(new SchedulerTaskManagerWrapper(taskManager));
    }

    // --------------------- 实现 SQLStatement 相关的代码 ---------------------

    @Override
    public void executeNextStatement() {
    }

    @Override
    public boolean yieldIfNeeded(PreparedSQLStatement current) {
        return false;
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

    @Override
    public void addSession(Session session) {
        addSession((InternalSession) session);
    }

    @Override
    public void removeSession(Session session) {
        removeSession((InternalSession) session);
    }
}
