/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionStatus;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionListener;
import org.lealone.transaction.WaitingTransaction;
import org.lealone.transaction.aote.log.LogSyncService;
import org.lealone.transaction.aote.log.RedoLogRecord;
import org.lealone.transaction.aote.log.UndoLog;

public class AOTransaction implements Transaction {

    private static final LinkedList<WaitingTransaction> EMPTY_LINKED_LIST = new LinkedList<>();

    // 以下几个public或包级别的字段是在其他地方频繁使用的，
    // 为了使用方便或节省一点点性能开销就不通过getter方法访问了
    final AOTransactionEngine transactionEngine;
    final long transactionId;
    final int isolationLevel;
    final LogSyncService logSyncService;
    long commitTimestamp;

    UndoLog undoLog = new UndoLog();
    Runnable asyncTask;

    private HashMap<String, Integer> savepoints;
    private Session session;
    private final boolean autoCommit;

    // 被哪个事务锁住记录了
    private volatile AOTransaction lockedBy;
    private long lockStartTime;
    // 有哪些事务在等待我释放锁
    private final AtomicReference<LinkedList<WaitingTransaction>> waitingTransactionsRef //
            = new AtomicReference<>(EMPTY_LINKED_LIST);

    private LinkedList<TransactionalValue> locks; // 行锁

    public AOTransaction(AOTransactionEngine engine, long tid, boolean autoCommit, int level) {
        transactionEngine = engine;
        transactionId = tid;
        isolationLevel = level;
        logSyncService = engine.getLogSyncService();
        this.autoCommit = autoCommit;
    }

    public UndoLog getUndoLog() {
        return undoLog;
    }

    void addLock(TransactionalValue tv) {
        if (locks == null)
            locks = new LinkedList<>();
        locks.add(tv);
    }

    @Override
    public void setSession(Session session) {
        this.session = session;
    }

    @Override
    public Session getSession() {
        return session;
    }

    public boolean isCommitted() {
        return commitTimestamp > 0;
    }

    @Override
    public boolean isClosed() {
        return undoLog == null;

    }

    @Override
    public int getIsolationLevel() {
        return isolationLevel;
    }

    public boolean isRepeatableRead() {
        return isolationLevel >= Transaction.IL_REPEATABLE_READ;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public <K, V> AOTransactionMap<K, V> openMap(String name, Storage storage) {
        return openMap(name, null, null, storage);
    }

    @Override
    public <K, V> AOTransactionMap<K, V> openMap(String name, StorageDataType keyType,
            StorageDataType valueType, Storage storage) {
        return openMap(name, keyType, valueType, storage, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> AOTransactionMap<K, V> openMap(String name, StorageDataType keyType,
            StorageDataType valueType, Storage storage, Map<String, String> parameters) {
        checkNotClosed();
        if (keyType == null)
            keyType = new ObjectDataType();
        if (valueType == null)
            valueType = new ObjectDataType();
        valueType = new TransactionalValueType(valueType);

        if (parameters == null)
            parameters = new HashMap<>(1);

        StorageMap<K, TransactionalValue> map = storage.openMap(name, keyType, valueType, parameters);
        if (!map.isInMemory()) {
            logSyncService.getRedoLog().redo(map);
        }
        transactionEngine.addStorageMap((StorageMap<Object, TransactionalValue>) map);
        return new AOTransactionMap<>(this, map);
    }

    @Override
    public void addSavepoint(String name) {
        if (savepoints == null)
            savepoints = new HashMap<>();

        savepoints.put(name, getSavepointId());
    }

    @Override
    public int getSavepointId() {
        return undoLog.getLogId();
    }

    @Override
    public void asyncCommit(Runnable asyncTask) {
        this.asyncTask = asyncTask;
        writeRedoLog(true);
    }

    private RedoLogRecord createLocalTransactionRedoLogRecord() {
        DataBuffer operations = undoLog.toRedoLogRecordBuffer(transactionEngine);
        if (operations == null || operations.limit() == 0)
            return null;
        return RedoLogRecord.createLocalTransactionRedoLogRecord(transactionId, operations);
    }

    // 如果不需要事务日志同步或者不需要立即做事务日志同步那么返回true，这时可以直接提交事务了。
    // 如果需要立即做事务日志，当需要异步提交事务时返回false，当需要同步提交时需要等待
    private void writeRedoLog(boolean asyncCommit) {
        checkNotClosed();
        // 在写redo log前先更新内存脏页，确保checkpoint线程保存脏页时使用的是新值
        commitTimestamp = transactionEngine.nextTransactionId(); // 生成新的
        undoLog.commit(transactionEngine); // 先提交，事务变成结束状态再解锁
        if (logSyncService.needSync() && undoLog.isNotEmpty()) {
            RedoLogRecord r = createLocalTransactionRedoLogRecord();
            if (r == null) {
                // 不需要事务日志同步，可以直接提交事务了
                if (asyncCommit)
                    asyncCommitComplete();
                return;
            }
            if (asyncCommit) {
                logSyncService.asyncCommit(r, this);
            } else {
                logSyncService.addAndMaybeWaitForSync(r);
            }
        } else {
            // 不需要事务日志同步，可以直接提交事务了
            if (asyncCommit)
                asyncCommitComplete();
        }
    }

    @Override
    public void asyncCommitComplete() {
        commitFinal();
        if (session != null) {
            session.asyncCommitComplete();
        }
        if (asyncTask != null) {
            try {
                asyncTask.run();
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    @Override
    public void commit() {
        writeRedoLog(false);
        commitFinal();
    }

    private void commitFinal() {
        // 避免重复提交
        if (!transactionEngine.containsTransaction(transactionId))
            return;
        endTransaction();
        // wakeUpWaitingTransactions(); //在session级调用
    }

    private void endTransaction() {
        savepoints = null;
        undoLog = null;
        transactionEngine.removeTransaction(transactionId);
        unlock();
    }

    // 无论是提交还是回滚都需要解锁
    private void unlock() {
        if (locks != null) {
            for (TransactionalValue tv : locks)
                tv.unlock();
            locks = null;
        }
    }

    @Override
    public void wakeUpWaitingTransactions() {
        while (true) {
            LinkedList<WaitingTransaction> waitingTransactions = waitingTransactionsRef.get();
            if (waitingTransactions != null && waitingTransactions != EMPTY_LINKED_LIST) {
                for (WaitingTransaction waitingTransaction : waitingTransactions) {
                    waitingTransaction.wakeUp();
                }
            }
            if (waitingTransactionsRef.compareAndSet(waitingTransactions, null))
                break;
        }
        lockedBy = null;
    }

    @Override
    public int addWaitingTransaction(Object key, Transaction t, TransactionListener listener) {
        // 如果已经提交了，通知重试
        if (isClosed())
            return OPERATION_NEED_RETRY;

        AOTransaction transaction = (AOTransaction) t;
        Session session = transaction.getSession();
        SessionStatus oldSessionStatus = session.getStatus();
        session.setStatus(SessionStatus.WAITING);
        transaction.waitFor(this);

        WaitingTransaction wt = new WaitingTransaction(key, transaction, listener);
        while (true) {
            LinkedList<WaitingTransaction> waitingTransactions = waitingTransactionsRef.get();
            // 如果已经提交了，通知重试
            if (isClosed() || waitingTransactions == null) {
                transaction.waitFor(null);
                session.setStatus(oldSessionStatus);
                return OPERATION_NEED_RETRY;
            }
            LinkedList<WaitingTransaction> newWaitingTransactions = new LinkedList<>(
                    waitingTransactions);
            newWaitingTransactions.add(wt);
            if (waitingTransactionsRef.compareAndSet(waitingTransactions, newWaitingTransactions)) {
                return OPERATION_NEED_WAIT;
            }
        }
    }

    private void waitFor(AOTransaction transaction) {
        lockedBy = transaction;
        lockStartTime = System.currentTimeMillis();
    }

    @Override
    public void checkTimeout() {
        if (lockedBy != null && lockStartTime != 0
                && System.currentTimeMillis() - lockStartTime > session.getLockTimeout()) {
            boolean isDeadlock = false;
            LinkedList<WaitingTransaction> waitingTransactions = waitingTransactionsRef.get();
            if (waitingTransactions == null)
                return;
            for (WaitingTransaction wt : waitingTransactions) {
                if (wt.getTransaction() == lockedBy) {
                    isDeadlock = true;
                    break;
                }
            }
            waitingTransactions = lockedBy.waitingTransactionsRef.get();
            if (waitingTransactions == null)
                return;
            WaitingTransaction waitingTransaction2 = null;
            for (WaitingTransaction wt : waitingTransactions) {
                if (wt.getTransaction() == this) {
                    waitingTransaction2 = wt;
                    break;
                }
            }
            String keyStr = waitingTransaction2.getKey().toString();
            if (isDeadlock) {
                String msg = getMsg(transactionId, session, lockedBy);
                msg += "\r\n" + getMsg(lockedBy.transactionId, lockedBy.session, this);

                msg += ", the locked object: " + keyStr;
                throw DbException.get(ErrorCode.DEADLOCK_1, msg);
            } else {
                String msg = getMsg(transactionId, session, lockedBy);
                throw DbException.get(ErrorCode.LOCK_TIMEOUT_1, keyStr, msg);
            }
        }
    }

    private static String getMsg(long tid, Session session, AOTransaction transaction) {
        return "transaction #" + tid + " in session " + session + " wait for transaction #"
                + transaction.transactionId + " in session " + transaction.session;
    }

    @Override
    public void rollback() {
        try {
            checkNotClosed();
            rollbackTo(0);
        } finally {
            endTransaction();
            // wakeUpWaitingTransactions(); //在session级调用
        }
    }

    @Override
    public void rollbackToSavepoint(String name) {
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }

        Integer savepointId = savepoints.get(name);
        if (savepointId == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        int i = savepointId.intValue();
        rollbackToSavepoint(i);

        if (savepoints != null) {
            String[] names = new String[savepoints.size()];
            savepoints.keySet().toArray(names);
            for (String n : names) {
                savepointId = savepoints.get(n);
                if (savepointId.longValue() >= i) {
                    savepoints.remove(n);
                }
            }
        }
    }

    @Override
    public void rollbackToSavepoint(int savepointId) {
        checkNotClosed();
        rollbackTo(savepointId);
    }

    private void rollbackTo(int toLogId) {
        undoLog.rollbackTo(transactionEngine, toLogId);
    }

    protected void checkNotClosed() {
        if (isClosed()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "Transaction is closed");
        }
    }

    @Override
    public String toString() {
        return "t[" + transactionId + ", " + autoCommit + "]";
    }
}
