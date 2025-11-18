/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import com.lealone.db.link.LinkableBase;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.storage.StorageMap;

public class PendingTransaction extends LinkableBase<PendingTransaction> {

    private final Transaction transaction;
    private final Object redoLogRecord;
    private final long logId;
    private CountDownLatch latch;
    private boolean synced;
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

    private ConcurrentHashMap<StorageMap<?, ?>, StorageMap<?, ?>> maps;

    public ConcurrentHashMap<StorageMap<?, ?>, StorageMap<?, ?>> getMaps() {
        return maps;
    }

    public void setMaps(ConcurrentHashMap<StorageMap<?, ?>, StorageMap<?, ?>> maps) {
        this.maps = maps;
    }
}
