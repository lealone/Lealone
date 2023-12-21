/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.RunMode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionStatus;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageSetting;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.lock.RowLock;
import org.lealone.transaction.aote.log.LogSyncService;
import org.lealone.transaction.aote.log.RedoLogRecord;
import org.lealone.transaction.aote.log.RedoLogRecord.LazyLocalTransactionRLR;
import org.lealone.transaction.aote.log.RedoLogRecord.LobSave;
import org.lealone.transaction.aote.log.RedoLogRecord.LocalTransactionRLR;
import org.lealone.transaction.aote.log.UndoLog;
import org.lealone.transaction.aote.tm.TransactionManager;

public class AOTransaction implements Transaction {

    // 以下几个public或包级别的字段是在其他地方频繁使用的，
    // 为了使用方便或节省一点点性能开销就不通过getter方法访问了
    public final AOTransactionEngine transactionEngine;
    public final long transactionId;
    public final String transactionName;
    public final LogSyncService logSyncService;
    protected volatile long commitTimestamp;

    protected UndoLog undoLog = new UndoLog();
    final RunMode runMode;
    protected Runnable asyncTask;

    private HashMap<String, Integer> savepoints;
    private Session session;
    private final int isolationLevel;
    private boolean autoCommit;

    private TransactionManager transactionManager;

    // 仅用于测试
    private LinkedList<RowLock> locks; // 行锁

    public AOTransaction(AOTransactionEngine engine, long tid, RunMode runMode, int level) {
        this(engine, tid, runMode, level, null);
    }

    public AOTransaction(AOTransactionEngine engine, long tid, RunMode runMode, int level,
            String hostAndPort) {
        transactionEngine = engine;
        transactionId = tid;
        transactionName = getTransactionName(hostAndPort, tid);
        isolationLevel = level;
        logSyncService = engine.getLogSyncService();
        this.runMode = runMode;
    }

    public AOTransactionEngine getTransactionEngine() {
        return transactionEngine;
    }

    public UndoLog getUndoLog() {
        return undoLog;
    }

    public void addLock(RowLock lock) {
        if (locks == null)
            locks = new LinkedList<>();
        locks.add(lock);
    }

    // 无论是提交还是回滚都需要解锁
    private void unlock() {
        if (locks != null) {
            for (RowLock lock : locks)
                lock.unlock(getSession(), true, null);
            locks = null;
        }
    }

    @Override
    public String getTransactionName() {
        return transactionName;
    }

    @Override
    public void setSession(Session session) {
        this.session = session;
        autoCommit = session.isAutoCommit();
    }

    @Override
    public Session getSession() {
        return session;
    }

    public boolean isUpdateCommand() {
        return session != null && !session.isQueryCommand();
    }

    public boolean isCommitted() {
        return commitTimestamp > 0;
    }

    @Override
    public boolean isClosed() {
        return undoLog == null;
    }

    @Override
    public boolean isWaiting() {
        return session != null && (session.getStatus() == SessionStatus.WAITING);
    }

    @Override
    public int getIsolationLevel() {
        return isolationLevel;
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
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public <K, V> TransactionMap<K, V> openMap(String name, Storage storage) {
        return openMap(name, null, null, storage);
    }

    @Override
    public <K, V> AOTransactionMap<K, V> openMap(String name, StorageDataType keyType,
            StorageDataType valueType, Storage storage) {
        return openMap(name, keyType, valueType, storage, null);
    }

    @Override
    public <K, V> AOTransactionMap<K, V> openMap(String name, StorageDataType keyType,
            StorageDataType valueType, Storage storage, Map<String, String> parameters) {
        checkNotClosed();
        if (keyType == null)
            keyType = new ObjectDataType();
        if (valueType == null)
            valueType = new ObjectDataType();
        valueType = new TransactionalValueType(valueType, storage.isByteStorage());

        if (parameters == null)
            parameters = new HashMap<>(1);
        if (runMode == RunMode.SHARDING && !parameters.containsKey(StorageSetting.RUN_MODE.name()))
            parameters.put(StorageSetting.RUN_MODE.name(), runMode.name());

        storage.registerEventListener(transactionEngine);
        StorageMap<K, TransactionalValue> map = storage.openMap(name, keyType, valueType, parameters);
        return createTransactionMap(map, parameters);
    }

    protected <K, V> AOTransactionMap<K, V> createTransactionMap(StorageMap<K, TransactionalValue> map,
            Map<String, String> parameters) {
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
        UndoLog ul = undoLog;
        if (ul == null)
            return 0;
        else
            return ul.getLogId();
    }

    protected Runnable lobTask;

    @Override
    public void addLobTask(Runnable lobTask) {
        this.lobTask = lobTask;
    }

    protected DataBuffer toRedoLogRecordBuffer() {
        return undoLog.toRedoLogRecordBuffer(transactionEngine, getScheduler().getDataBufferFactory());
    }

    private RedoLogRecord createLocalTransactionRedoLogRecord() {
        if (logSyncService.isPeriodic()) {
            // 当前线程省一点事，让redo log sync线程把undo log编码为redo log
            return new LazyLocalTransactionRLR(undoLog, transactionEngine);
        } else {
            return new LocalTransactionRLR(toRedoLogRecordBuffer());
        }
    }

    private void writeRedoLog(boolean asyncCommit) {
        checkNotClosed();
        if (logSyncService.needSync() && undoLog.isNotEmpty()) {
            long logId = logSyncService.nextLogId();
            RedoLogRecord r = createLocalTransactionRedoLogRecord();
            if (lobTask != null)
                r = new LobSave(lobTask, r);
            if (asyncCommit) {
                logSyncService.asyncWrite(this, r, logId);
            } else {
                logSyncService.syncWrite(this, r, logId);
            }
        } else {
            if (lobTask != null)
                lobTask.run();
            if (undoLog.isNotEmpty()) // 只读事务不用管
                onSynced();
            // 不需要事务日志同步，可以直接提交事务了
            if (asyncCommit)
                asyncCommitComplete();
        }
    }

    @Override
    public void onSynced() {
        if (commitTimestamp > 0)
            return;
        // 这一步很重要！！！
        // 生成commitTimestamp的时机很严格，需要等到redo log sync完成后才能生成，
        // checkpoint线程和可重复读的事务都依赖它
        commitTimestamp = transactionEngine.nextTransactionId();
    }

    @Override
    public void asyncCommit(Runnable asyncTask) {
        this.asyncTask = asyncTask;
        writeRedoLog(true);
    }

    @Override
    public void asyncCommitComplete() {
        commitFinal();
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
        commitLocal();
    }

    private void commitLocal() {
        writeRedoLog(false);
        commitFinal();
    }

    protected void commitFinal() {
        commitFinal(transactionId, bitIndex);
    }

    // tid在分布式场景下可能是其他事务的tid
    protected void commitFinal(long tid, int bitIndex) {
        // 避免并发提交(TransactionValidator线程和其他读写线程都有可能在检查到分布式事务有效后帮助提交最终事务)
        AOTransaction t = transactionManager.removeTransaction(tid, bitIndex);
        if (t == null)
            return;
        t.undoLog.commit(transactionEngine); // 先提交，事务变成结束状态再解锁
        // 在删除事务前标记脏页，这样GC线程看到事务没结束就不会对脏页进行GC
        if (t.session != null)
            t.session.markDirtyPages();
        t.endTransaction(false);
    }

    private void endTransaction(boolean remove) {
        savepoints = null;
        undoLog = null;
        if (remove)
            transactionManager.removeTransaction(transactionId, bitIndex);
        unlock();
    }

    @Override
    public int addWaitingTransaction(Object key, Session session,
            AsyncHandler<SessionStatus> asyncHandler) {
        // 如果已经提交了，通知重试
        if (isClosed() || isWaiting())
            return OPERATION_NEED_RETRY;
        if (session == null) // 单元测试时session为null
            return OPERATION_NEED_WAIT;

        SessionStatus oldSessionStatus = session.getStatus();
        if (asyncHandler != null) {
            asyncHandler.handle(SessionStatus.WAITING);
        } else {
            session.setLockedBy(SessionStatus.WAITING, this, key);
        }

        this.session.addWaitingScheduler(session.getScheduler());

        // 如果已经提交了，要恢复到原来的状态，通知重试
        if (isClosed() || isWaiting()) {
            if (asyncHandler != null) {
                asyncHandler.handle(null);
            } else {
                session.setLockedBy(oldSessionStatus, null, null);
            }
            return OPERATION_NEED_RETRY;
        }
        return OPERATION_NEED_WAIT;
    }

    @Override
    public void rollback() {
        try {
            checkNotClosed();
            rollbackTo(0);
        } finally {
            endTransaction(true);
            // 在session级唤醒等待的事务
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
        return "t[" + transactionName + ", " + autoCommit + "]";
    }

    public static String getTransactionName(String hostAndPort, long tid) {
        if (hostAndPort == null)
            hostAndPort = "0:0";
        StringBuilder buff = new StringBuilder(hostAndPort);
        buff.append(':');
        buff.append(tid);
        return buff.toString();
    }

    public <T> AsyncCallback<T> createCallback() {
        return session != null ? session.createCallback() : AsyncCallback.createConcurrentCallback();
    }

    protected Scheduler scheduler;

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    protected int bitIndex;

    public int getBitIndex() {
        return bitIndex;
    }

    public void setBitIndex(int bitIndex) {
        this.bitIndex = bitIndex;
    }
}
