/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.db.Database;
import com.lealone.db.async.AsyncPeriodicTask;
import com.lealone.db.result.Row;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.StandardTable;
import com.lealone.db.value.Value;

public class IndexOperator implements Runnable {

    private final Scheduler scheduler;
    private final StandardTable table;
    private final ServerSession session;
    private final AsyncPeriodicTask task;

    private final LinkedTransferQueue<IndexOperation> indexOperations = new LinkedTransferQueue<>();
    private final AtomicLong indexOperationSize = new AtomicLong(0);

    public IndexOperator(Scheduler scheduler, StandardTable table) {
        this.scheduler = scheduler;
        this.table = table;
        Database db = table.getDatabase();
        session = db.createSession(db.getSystemUser(), scheduler);
        session.setUndoLogEnabled(false);
        task = new AsyncPeriodicTask(0, 100, this);
        scheduler.addPeriodicTask(task);
    }

    public IndexOperation addRowLazy(long rowKey, Value[] columns) {
        return new AIO(rowKey, columns);
    }

    public IndexOperation updateRowLazy(long oldRowKey, long newRowKey, Value[] oldColumns,
            Value[] newColumns, int[] updateColumns) {
        return new UIO(oldRowKey, newRowKey, oldColumns, newColumns, updateColumns);
    }

    public IndexOperation removeRowLazy(long rowKey, Value[] columns) {
        return new RIO(rowKey, columns);
    }

    public void addIndexOperation(ServerSession session, IndexOperation io) {
        if (io.rowKey == 0)
            io.rowKey = session.getLastIdentity();
        indexOperations.add(io);
        indexOperationSize.incrementAndGet();
    }

    private void cancelTask() {
        task.cancel();
        scheduler.removePeriodicTask(task);
    }

    @Override
    public void run() {
        if (table.isInvalid()) { // 比如已经drop了
            cancelTask();
            return;
        }
        if (indexOperationSize.get() <= 0)
            return;
        try {
            session.getTransaction();
            int i = 0;
            while (true) {
                IndexOperation io = indexOperations.poll();
                if (io == null)
                    return;
                indexOperationSize.decrementAndGet();
                try {
                    io.run(table, session);
                } catch (Exception e) {
                    if (table.isInvalid()) {
                        cancelTask();
                    }
                    break;
                }
                if ((++i & 127) == 0) {
                    if (scheduler.yieldIfNeeded(null))
                        return;
                }
            }
        } finally {
            session.asyncCommit();
        }
    }

    public static abstract class IndexOperation {

        long rowKey; // addRow的场景需要回填
        final Value[] columns;

        public IndexOperation(long rowKey, Value[] columns) {
            this.rowKey = rowKey;
            this.columns = columns;
        }

        public abstract void run(StandardTable table, ServerSession session);
    }

    private static class AIO extends IndexOperation {

        public AIO(long rowKey, Value[] columns) {
            super(rowKey, columns);
        }

        @Override
        public void run(StandardTable table, ServerSession session) {
            table.addRowAsync(session, new Row(rowKey, columns)).onComplete(ar -> {
            });
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
        public void run(StandardTable table, ServerSession session) {
            table.updateRowAsync(session, new Row(oldRowKey, oldColumns), new Row(rowKey, columns),
                    updateColumns, true).onComplete(ar -> {
                    });
        }
    }

    private static class RIO extends IndexOperation {

        public RIO(long rowKey, Value[] columns) {
            super(rowKey, columns);
        }

        @Override
        public void run(StandardTable table, ServerSession session) {
            table.removeRowAsync(session, new Row(rowKey, columns), true).onComplete(ar -> {
            });
        }
    }
}
