/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.db.link.LinkableBase;
import com.lealone.db.scheduler.InternalScheduler;

public class PendingTransaction extends LinkableBase<PendingTransaction> {

    private static final AtomicLong globalCount = new AtomicLong();
    private static int limit; // 限制待处理的事务数，避免出内存问题

    public static void increment() {
        globalCount.incrementAndGet();
    }

    public static void decrement() {
        globalCount.decrementAndGet();
    }

    public static void setLimit(int limit) {
        PendingTransaction.limit = limit;
    }

    public static boolean isExceeded() {
        return globalCount.get() > limit;
    }

    private final Transaction transaction;
    private final Object redoLogRecord;

    private final long logId;
    private CountDownLatch latch;
    private volatile boolean synced;
    private boolean completed;

    public PendingTransaction(Transaction transaction, Object redoLogRecord, long logId) {
        this.transaction = transaction;
        this.redoLogRecord = redoLogRecord;
        this.logId = logId;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public Object getRedoLogRecord() {
        return redoLogRecord;
    }

    public long getLogId() {
        return logId;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public boolean isSynced() {
        return synced;
    }

    public void setSynced(boolean synced) {
        if (transaction != null)
            transaction.onSynced();
        this.synced = synced;
        if (latch != null)
            latch.countDown();
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public InternalScheduler getScheduler() {
        return transaction.getScheduler();
    }
}
