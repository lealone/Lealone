/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DataBuffer;
import org.lealone.db.RunMode;
import org.lealone.net.NetNode;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.aote.log.RedoLogRecord;

public class AOTransaction extends AMTransaction {

    private List<Participant> participants;
    private String globalTransactionName;

    AOTransaction(AOTransactionEngine engine, long tid) {
        super(engine, tid, NetNode.getLocalTcpHostAndPort());
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public void addParticipant(Participant participant) {
        if (participants == null)
            participants = new ArrayList<>();
        participants.add(participant);
    }

    @Override
    protected <K, V> AMTransactionMap<K, V> createTransactionMap(StorageMap<K, TransactionalValue> map,
            Map<String, String> parameters) {
        if (runMode == RunMode.SHARDING)
            return new DTransactionMap<>(this, map);
        else
            return new AOTransactionMap<>(this, map);
    }

    @Override
    public void asyncCommit(Runnable asyncTask) {
        checkNotClosed();
        this.asyncTask = asyncTask;
        commit(null, true);
    }

    @Override
    public void asyncCommitComplete() {
        if (asyncTask != null) {
            try {
                asyncTask.run();
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
        if (isAutoCommit())
            getSession().asyncCommitComplete();
    }

    @Override
    public void commit() {
        checkNotClosed();
        commit(null, false);
    }

    @Override
    public void commit(String globalTransactionName) {
        checkNotClosed();
        commit(globalTransactionName, false);
    }

    private void commit(String globalTransactionName, boolean asyncCommit) {
        if (globalTransactionName == null)
            globalTransactionName = getTransactionName();
        this.globalTransactionName = globalTransactionName;
        long commitTimestamp = transactionEngine.nextOddTransactionId();
        RedoLogRecord r = createDistributedTransactionRedoLogRecord(globalTransactionName, commitTimestamp);
        if (r != null) { // 事务没有进行任何操作时不用同步日志
            // 先写redoLog
            if (asyncCommit) {
                logSyncService.addRedoLogRecord(r);
                logSyncService.asyncCommit(this);
            } else {
                logSyncService.addAndMaybeWaitForSync(r);
            }
        } else if (asyncCommit) {
            asyncCommitComplete();
        }

        if (!isAutoCommit())
            DTRValidator.addTransaction(this, globalTransactionName, commitTimestamp);

        if (participants != null && !isAutoCommit()) {
            List<Participant> participants = this.participants;
            this.participants = null;
            AtomicInteger size = new AtomicInteger(participants.size());
            for (Participant participant : participants) {
                participant.commitTransaction(globalTransactionName).onComplete(ar -> {
                    if (size.decrementAndGet() == 0) {
                        for (Participant p : participants) {
                            p.commitFinal();
                        }
                        getSession().commitFinal();
                    }
                });
            }
        }
    }

    private RedoLogRecord createDistributedTransactionRedoLogRecord(String globalTransactionName,
            long commitTimestamp) {
        DataBuffer operations = getUndoLog().toRedoLogRecordBuffer(transactionEngine);
        if (operations == null)
            return null;
        return RedoLogRecord.createDistributedTransactionRedoLogRecord(transactionId, transactionName,
                globalTransactionName, commitTimestamp, operations);
    }

    @Override
    protected void commitFinal(long tid) {
        super.commitFinal(tid);
        DTRValidator.removeTransaction(this, globalTransactionName);
        if (globalReplicationName != null)
            DTRValidator.removeReplication(globalReplicationName);
    }

    @Override
    public void rollback() {
        if (participants != null && !isAutoCommit()) {
            for (Participant participant : participants) {
                participant.rollbackTransaction();
            }
        }
    }

    @Override
    public void addSavepoint(String name) {
        super.addSavepoint(name);
        if (participants != null && !isAutoCommit()) {
            for (Participant participant : participants) {
                participant.addSavepoint(name);
            }
        }
    }

    @Override
    public void rollbackToSavepoint(String name) {
        super.rollbackToSavepoint(name);
        if (participants != null && !isAutoCommit()) {
            for (Participant participant : participants) {
                participant.rollbackToSavepoint(name);
            }
        }
    }
}
