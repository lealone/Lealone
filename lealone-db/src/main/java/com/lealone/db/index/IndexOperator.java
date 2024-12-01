/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.db.Database;
import com.lealone.db.DbObjectType;
import com.lealone.db.async.AsyncPeriodicTask;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.link.LinkableBase;
import com.lealone.db.link.LinkableList;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.row.Row;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerTaskManager;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.StandardTable;
import com.lealone.db.value.Value;
import com.lealone.transaction.Transaction;

public class IndexOperator extends SchedulerTaskManager implements Runnable {

    private final InternalScheduler scheduler;
    private final StandardTable table;
    private final Index index;
    private final ServerSession session; // 这个session只用于加锁
    private final ServerSession ioSession; // 这个session只用于执行索引操作
    private final AsyncPeriodicTask task;

    private final LinkableList<IndexOperation>[] pendingIosArray;
    private final AtomicLong indexOperationSize = new AtomicLong(0);
    private final DbObjectLock lock = new DbObjectLock(DbObjectType.INDEX);

    @SuppressWarnings("unchecked")
    public IndexOperator(InternalScheduler scheduler, StandardTable table, Index index) {
        this.scheduler = scheduler;
        this.table = table;
        this.index = index;
        Database db = table.getDatabase();
        session = db.createSession(db.getSystemUser(), scheduler);
        ioSession = db.createSession(db.getSystemUser(), scheduler);
        ioSession.setRedoLogEnabled(false);
        task = new AsyncPeriodicTask(0, 100, this);
        scheduler.addPeriodicTask(task);
        pendingIosArray = new LinkableList[scheduler.getSchedulerFactory().getSchedulerCount()];
    }

    public boolean hasPendingIndexOperation() {
        return indexOperationSize.get() > 0;
    }

    private void addIndexOperation(InternalScheduler currentScheduler, IndexOperation io) {
        LinkableList<IndexOperation> pendingIos = pendingIosArray[currentScheduler.getId()];
        if (pendingIos == null) {
            pendingIos = new LinkableList<>();
            pendingIosArray[currentScheduler.getId()] = pendingIos;
            currentScheduler.addTaskManager(this);
        }
        pendingIos.add(io);
        indexOperationSize.incrementAndGet();
        if (currentScheduler != scheduler)
            scheduler.wakeUp();
    }

    @Override
    public boolean gcCompletedTasks(InternalScheduler scheduler) {
        LinkableList<IndexOperation> pendingIos = pendingIosArray[scheduler.getId()];
        if (pendingIos == null)
            return true;
        else if (pendingIos.isEmpty()) {
            pendingIosArray[scheduler.getId()] = null;
            return true;
        }
        IndexOperation io = pendingIos.getHead();
        while (io != null && io.isCompleted()) {
            io = io.getNext();
            pendingIos.decrementSize();
            pendingIos.setHead(io);
        }
        if (pendingIos.getHead() == null) {
            pendingIos.setTail(null);
            pendingIosArray[scheduler.getId()] = null;
            return true;
        }
        return false;
    }

    private void cancelTask() {
        task.cancel();
        scheduler.removePeriodicTask(task);
        for (int i = 0; i < pendingIosArray.length; i++)
            pendingIosArray[i] = null;
        indexOperationSize.set(0);
    }

    @Override
    public void run() {
        run(session);
    }

    public void run(ServerSession session) {
        if (indexOperationSize.get() <= 0)
            return;
        if (!lock.tryExclusiveLock(session)) {
            if (session != this.session)
                throw DbObjectLock.LOCKED_EXCEPTION;
            return;
        }
        if (table.isInvalid()) { // 比如已经drop了
            cancelTask();
            return;
        }
        int count = 0;
        try {
            AtomicInteger index = new AtomicInteger(0);
            Scheduler[] schedulers = scheduler.getSchedulerFactory().getSchedulers();
            int schedulerCount = pendingIosArray.length;
            while (indexOperationSize.get() > 0) {
                long lastSize = indexOperationSize.get();
                IndexOperation[] lastIos = new IndexOperation[schedulerCount];
                IndexOperation[] ios = new IndexOperation[schedulerCount];
                outer: for (int i = 0; i < schedulerCount; i++) {
                    LinkableList<IndexOperation> pendingIos = pendingIosArray[i];
                    if (pendingIos == null)
                        continue;
                    IndexOperation io = pendingIos.getHead();
                    while (io != null) {
                        if (io.isCompleted()) {
                            io = io.getNext();
                            continue;
                        }
                        int status = io.getStatus();
                        if (status == 0)
                            continue outer; // 未提交，不能进行后续处理
                        else if (status < 0) {
                            io.setCompleted(true); // 已经回滚，直接废弃
                            continue;
                        }
                        ios[i] = io;
                        break;
                    }
                }
                // 找出提交时间戳最小的IndexOperation
                IndexOperation io = nextIndexOperation(ios, index);
                while (io != null) {
                    run(io);
                    int i = index.get();
                    lastIos[i] = io;
                    while (true) {
                        io = io.getNext();
                        if (io == null) {
                            ios[i] = null;
                            break;
                        }
                        // 索引操作链表中的下一个索引操作对应的父事务若是没有提交就不能执行它了
                        int status = io.getStatus();
                        if (status == 0) {
                            ios[i] = null; // 未提交，不能进行后续处理
                            break;
                        } else if (status < 0) {
                            io.setCompleted(true); // 已经回滚，直接废弃
                            continue;
                        } else {
                            ios[i] = io;
                            break;
                        }
                    }
                    if ((++count & 127) == 0) {
                        if (scheduler.yieldIfNeeded(null))
                            return;
                    }
                    io = nextIndexOperation(ios, index);
                }
                for (int i = 0; i < schedulerCount; i++) {
                    if (lastIos[i] == null) {
                        continue;
                    }
                    schedulers[i].wakeUp();
                }
                // 所有的事务都没提交，直接退出
                if (lastSize == indexOperationSize.get())
                    break;
            }
        } finally {
            if (session == this.session) {
                session.asyncCommit();
            } else {
                session.removeLock(lock);
                lock.unlockFast();
            }
        }
    }

    private IndexOperation nextIndexOperation(IndexOperation[] ios, AtomicInteger index) {
        IndexOperation minIo = null;
        long minCommitTimestamp = Long.MAX_VALUE;
        for (int i = 0, len = ios.length; i < len; i++) {
            IndexOperation io = ios[i];
            while (io != null) {
                if (io.isCompleted()) {
                    io = io.getNext();
                    ios[i] = io;
                    continue;
                }
                if (io.getTransaction().getCommitTimestamp() < minCommitTimestamp) {
                    minCommitTimestamp = io.getTransaction().getCommitTimestamp();
                    minIo = io;
                    index.set(i);
                }
                break;
            }
        }
        return minIo;
    }

    private void run(IndexOperation io) {
        Transaction transaction = ioSession.getTransaction();
        transaction.setParentTransaction(io.getTransaction());
        try {
            io.run(table, index, ioSession);
        } catch (Exception e) {
            if (table.isInvalid()) {
                cancelTask();
            }
        } finally {
            io.setCompleted(true);
            indexOperationSize.decrementAndGet();
            ioSession.asyncCommit();
        }
    }

    public static void addIndexOperation(ServerSession session, StandardTable table, IndexOperation io) {
        if (io.rowKey == 0)
            io.rowKey = session.getLastIdentity();
        List<IndexOperator> indexOperators = table.getIndexOperators();
        for (int i = 0, size = indexOperators.size(); i < size; i++) {
            indexOperators.get(i).addIndexOperation(session.getScheduler(), (i == 0 ? io : io.copy()));
        }
    }

    public static IndexOperation addRowLazy(long rowKey, Value[] columns) {
        return new AIO(rowKey, columns);
    }

    public static IndexOperation updateRowLazy(long oldRowKey, long newRowKey, Value[] oldColumns,
            Value[] newColumns, int[] updateColumns) {
        return new UIO(oldRowKey, newRowKey, oldColumns, newColumns, updateColumns);
    }

    public static IndexOperation removeRowLazy(long rowKey, Value[] columns) {
        return new RIO(rowKey, columns);
    }

    public static abstract class IndexOperation extends LinkableBase<IndexOperation> {

        long rowKey; // addRow的场景需要回填
        final Value[] columns;

        Transaction transaction;
        int savepointId;

        public IndexOperation(long rowKey, Value[] columns) {
            this.rowKey = rowKey;
            this.columns = columns;
        }

        public int getSavepointId() {
            return savepointId;
        }

        public Transaction getTransaction() {
            return transaction;
        }

        public void setTransaction(Transaction transaction) {
            this.transaction = transaction;
            this.savepointId = transaction.getSavepointId();
        }

        public int getStatus() {
            return transaction.getStatus(savepointId);
        }

        private boolean completed;

        public boolean isCompleted() {
            return completed;
        }

        public void setCompleted(boolean completed) {
            this.completed = completed;
        }

        public abstract void run(StandardTable table, Index index, ServerSession session);

        public abstract IndexOperation copy();

        public IndexOperation copy(IndexOperation io) {
            io.transaction = transaction;
            io.savepointId = savepointId;
            return io;
        }
    }

    private static class AIO extends IndexOperation {

        public AIO(long rowKey, Value[] columns) {
            super(rowKey, columns);
        }

        @Override
        public void run(StandardTable table, Index index, ServerSession session) {
            index.add(session, new Row(rowKey, columns), AsyncResultHandler.emptyHandler());
        }

        @Override
        public IndexOperation copy() {
            return super.copy(new AIO(rowKey, columns));
        }
    }

    private static class UIO extends IndexOperation {

        final long oldRowKey;
        final Value[] oldColumns;
        final int[] updateColumns;

        public UIO(long oldRowKey, long newRowKey, Value[] oldColumns, Value[] newColumns,
                int[] updateColumns) {
            super(newRowKey, newColumns);
            this.oldRowKey = oldRowKey;
            this.oldColumns = oldColumns;
            this.updateColumns = updateColumns;
        }

        @Override
        public void run(StandardTable table, Index index, ServerSession session) {
            index.update(session, new Row(oldRowKey, oldColumns), new Row(rowKey, columns), oldColumns,
                    updateColumns, true, AsyncResultHandler.emptyHandler());
        }

        @Override
        public IndexOperation copy() {
            return super.copy(new UIO(oldRowKey, rowKey, oldColumns, columns, updateColumns));
        }
    }

    private static class RIO extends IndexOperation {

        public RIO(long rowKey, Value[] columns) {
            super(rowKey, columns);
        }

        @Override
        public void run(StandardTable table, Index index, ServerSession session) {
            index.remove(session, new Row(rowKey, columns), columns, true,
                    AsyncResultHandler.emptyHandler());
        }

        @Override
        public IndexOperation copy() {
            return super.copy(new RIO(rowKey, columns));
        }
    }
}
