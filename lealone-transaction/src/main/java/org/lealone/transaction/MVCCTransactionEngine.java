/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.util.DataUtils;
import org.lealone.common.util.New;
import org.lealone.db.Constants;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.storage.type.WriteBuffer;
import org.lealone.transaction.log.LogChunkMap;
import org.lealone.transaction.log.LogMap;
import org.lealone.transaction.log.LogStorage;
import org.lealone.transaction.log.LogStorageBuilder;
import org.lealone.transaction.log.RedoLogValue;
import org.lealone.transaction.log.RedoLogValueType;

/**
 * The transaction engine that supports concurrent MVCC read-committed transactions.
 */
public class MVCCTransactionEngine extends TransactionEngineBase {

    private final ConcurrentHashMap<Integer, StorageMap<Object, VersionedValue>> maps = new ConcurrentHashMap<>();
    private final AtomicInteger lastTransactionId = new AtomicInteger();
    private int maxTransactionId = 0xffff;

    /**
     * The next id of a temporary map.
     */
    private int nextTempMapId;

    private boolean init;
    private boolean isClusterMode;
    String hostAndPort;

    private LogStorage logStorage;

    /**
     * The undo log.
     * <p>
     * If the first entry for a transaction doesn't have a logId
     * of 0, then the transaction is partially committed (which means rollback
     * is not possible). Log entries are written before the data is changed
     * (write-ahead).
     * <p>
     * Key: opId, value: [ mapId, key, oldValue ].
     */
    LogMap<Long, Object[]> undoLog;

    /**
     * The redo log.
     * 
     * Key: opId, value: [ mapId, key, newValue ].
     */
    LogMap<Long, RedoLogValue> redoLog;

    public MVCCTransactionEngine() {
        super(Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
    }

    StorageMap<Object, VersionedValue> getMap(int mapId) {
        return maps.get(mapId);
    }

    void addMap(StorageMap<Object, VersionedValue> map) {
        maps.put(map.getId(), map);
    }

    void removeMap(int mapId) {
        maps.remove(mapId);
    }

    /**
     * Initialize the store. This is needed before a transaction can be opened.
     * If the transaction store is corrupt, this method can throw an exception,
     * in which case the store can only be used for reading.
     */
    @Override
    public synchronized void init(Map<String, String> config) {
        if (init)
            return;
        init = true;
        isClusterMode = Boolean.parseBoolean(config.get("is_cluster_mode"));
        hostAndPort = config.get("host_and_port");

        LogStorageBuilder builder = new LogStorageBuilder();
        // TODO 指定LogStorageBuilder各类参数
        String baseDir = config.get("base_dir");
        String logDir = config.get("transaction_log_dir");
        String storageName = baseDir + File.separator + logDir;
        builder.storageName(storageName);
        logStorage = builder.open();

        // undoLog中存放的是所有事务的事务日志，
        // 就算是在同一个事务中也可能涉及同一个数据库中的多个表甚至多个数据库的多个表，
        // 所以要序列化数据，只能用ObjectDataType
        VersionedValueType oldValueType = new VersionedValueType(new ObjectDataType());
        ArrayType undoLogValueType = new ArrayType(new DataType[] { new ObjectDataType(), new ObjectDataType(),
                oldValueType });
        undoLog = logStorage.openLogMap("undoLog", new ObjectDataType(), undoLogValueType);
        if (undoLog.getValueType() != undoLogValueType) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_CORRUPT,
                    "Undo map open with a different value type");
        }

        redoLog = logStorage.openLogMap("redoLog", new ObjectDataType(), new RedoLogValueType());

        initTransactions();

        Long key = redoLog.lastKey();
        if (key != null)
            lastTransactionId.set(getTransactionId(key));

        // TODO 在这里删除logStorage中的临时表

        TransactionStatusTable.init(logStorage);

        if (isClusterMode)
            TransactionValidator.getInstance().start();
    }

    private void initTransactions() {
        List<Transaction> list = getOpenTransactions();
        for (Transaction t : list) {
            if (t.getStatus() == Transaction.STATUS_COMMITTING) {
                t.commit();
            } else {
                t.rollback();
            }
        }
    }

    /**
     * Get the list of unclosed transactions that have pending writes.
     *
     * @return the list of transactions (sorted by id)
     */
    private List<Transaction> getOpenTransactions() {
        ArrayList<Transaction> list = New.arrayList();
        Long key = undoLog.firstKey();
        while (key != null) {
            int transactionId = getTransactionId(key);
            key = undoLog.lowerKey(getOperationId(transactionId + 1, 0));
            long logId = getLogId(key) + 1;
            int status;
            if (undoLog.containsKey(getOperationId(transactionId, 0))) {
                status = MVCCTransaction.STATUS_OPEN;
            } else {
                status = MVCCTransaction.STATUS_COMMITTING;
            }
            MVCCTransaction t = new MVCCTransaction(this, transactionId, status, logId);
            list.add(t);
            key = undoLog.ceilingKey(getOperationId(transactionId + 1, 0));
        }
        return list;
    }

    /**
     * Begin a new transaction.
     *
     * @return the transaction
     */
    @Override
    public MVCCTransaction beginTransaction(boolean autoCommit) {
        if (!init) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE, "Not initialized");
        }
        int tid = nextTransactionId(autoCommit);
        MVCCTransaction t = new MVCCTransaction(this, tid, MVCCTransaction.STATUS_OPEN, 0);
        t.setAutoCommit(autoCommit);
        return t;
    }

    @Override
    public void close() {
        if (isClusterMode)
            TransactionValidator.getInstance().close();
    }

    private int nextTransactionId(boolean autoCommit) {
        // 分布式事务使用奇数的事务ID
        if (!autoCommit && isClusterMode) {
            return nextOddTransactionId();
        }

        return nextEvenTransactionId();
    }

    private int nextOddTransactionId() {
        int oldLast;
        int last;
        int delta;
        do {
            oldLast = lastTransactionId.get();
            last = oldLast;
            if (last % 2 == 0)
                delta = 1;
            else
                delta = 2;

            last += delta;

            if (last >= maxTransactionId)
                last = 1;
        } while (!lastTransactionId.compareAndSet(oldLast, last));
        return last;
    }

    private int nextEvenTransactionId() {
        int oldLast;
        int last;
        int delta;
        do {
            oldLast = lastTransactionId.get();
            last = oldLast;
            if (last % 2 == 0)
                delta = 2;
            else
                delta = 1;

            last += delta;

            if (last >= maxTransactionId)
                last = 2;
        } while (!lastTransactionId.compareAndSet(oldLast, last));
        return last;
    }

    /**
     * Set the maximum transaction id, after which ids are re-used. If the old
     * transaction is still in use when re-using an old id, the new transaction
     * fails.
     *
     * @param max the maximum id
     */
    public void setMaxTransactionId(int max) {
        this.maxTransactionId = max;
    }

    /**
     * Combine the transaction id and the log id to an operation id.
     *
     * @param transactionId the transaction id
     * @param logId the log id
     * @return the operation id
     */
    static long getOperationId(int transactionId, long logId) {
        DataUtils.checkArgument(transactionId >= 0 && transactionId < (1 << 24), "Transaction id out of range: {0}",
                transactionId);
        DataUtils.checkArgument(logId >= 0 && logId < (1L << 40), "Transaction log id out of range: {0}", logId);
        return ((long) transactionId << 40) | logId;
    }

    /**
     * Get the transaction id for the given operation id.
     *
     * @param operationId the operation id
     * @return the transaction id
     */
    static int getTransactionId(long operationId) {
        return (int) (operationId >>> 40);
    }

    /**
     * Get the log id for the given operation id.
     *
     * @param operationId the operation id
     * @return the log id
     */
    static long getLogId(long operationId) {
        return operationId & ((1L << 40) - 1);
    }

    /**
     * Log an entry.
     *
     * @param t the transaction
     * @param logId the log id
     * @param mapId the map id
     * @param key the key
     * @param oldValue the old value
     */
    void log(MVCCTransaction t, long logId, int mapId, Object key, Object oldValue) {
        Long undoKey = getOperationId(t.transactionId, logId);
        Object[] log = new Object[] { mapId, key, oldValue };
        synchronized (undoLog) {
            if (logId == 0) {
                if (undoLog.containsKey(undoKey)) {
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_STILL_OPEN,
                            "An old transaction with the same id " + "is still open: {0}", t.transactionId);
                }
            }
            undoLog.put(undoKey, log);
        }
    }

    /**
     * Remove a log entry.
     *
     * @param t the transaction
     * @param logId the log id
     */
    void logUndo(MVCCTransaction t, long logId) {
        Long undoKey = getOperationId(t.transactionId, logId);
        synchronized (undoLog) {
            Object[] old = undoLog.remove(undoKey);
            if (old == null) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_TRANSACTION_ILLEGAL_STATE,
                        "Transaction {0} was concurrently rolled back", t.transactionId);
            }
        }
    }

    /**
     * Commit a transaction.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     */
    void commit(MVCCTransaction t, long maxLogId) {
        // 分布式事务推迟删除undoLog
        if (t.transactionId % 2 == 0) {
            removeUndoLog(t.transactionId, maxLogId);
        }

        endTransaction(t);
    }

    void commitAfterValidate(int tid) {
        removeUndoLog(tid, Long.MAX_VALUE);
    }

    private void redoLog(Long operationId, int mapId, Object key, VersionedValue value) {
        WriteBuffer writeBuffer = LogChunkMap.getWriteBuffer();
        StorageMap<?, ?> map = maps.get(mapId);

        map.getKeyType().write(writeBuffer, key);
        ByteBuffer keyBuffer = writeBuffer.getBuffer();
        keyBuffer.flip();
        keyBuffer = keyBuffer.duplicate();

        writeBuffer.clear();

        ByteBuffer valueBuffer;
        if (value != null) {
            ((VersionedValueType) map.getValueType()).valueType.write(writeBuffer, value.value);
            valueBuffer = writeBuffer.getBuffer();
            valueBuffer.flip();
            valueBuffer = valueBuffer.duplicate();
        } else {
            valueBuffer = LogChunkMap.EMPTY_BUFFER;
        }

        RedoLogValue v = new RedoLogValue(mapId, keyBuffer, valueBuffer);
        redoLog.put(operationId, v);

        LogChunkMap.releaseWriteBuffer(writeBuffer);
    }

    @SuppressWarnings("unchecked")
    private void removeUndoLog(int tid, long maxLogId) {
        // TODO could synchronize on blocks (100 at a time or so)
        synchronized (undoLog) {
            ArrayList<Object[]> logs = new ArrayList<>();
            for (long logId = 0; logId < maxLogId; logId++) {
                Long undoKey = getOperationId(tid, logId);
                Object[] op = undoLog.get(undoKey);
                if (op == null) {
                    // partially committed: load next
                    undoKey = undoLog.ceilingKey(undoKey);
                    if (undoKey == null || getTransactionId(undoKey) != tid) {
                        break;
                    }
                    logId = getLogId(undoKey) - 1;
                    continue;
                }
                int mapId = (Integer) op[0];
                StorageMap<Object, VersionedValue> map = getMap(mapId);
                if (map == null) {
                    // map was later removed
                } else {
                    Object key = op[1];
                    VersionedValue value = map.get(key);
                    if (value == null) {
                        // nothing to do
                    } else if (value.value == null) {
                        redoLog(undoKey, mapId, key, null);
                        // remove the value
                        // map.remove(key);
                        logs.add(new Object[] { map, key, undoKey });
                    } else {
                        VersionedValue v2 = new VersionedValue();
                        v2.value = value.value;
                        redoLog(undoKey, mapId, key, v2);
                        // map.put(key, v2);
                        logs.add(new Object[] { map, key, v2, undoKey });
                    }
                }
                // undoLog.remove(undoKey);
            }

            // 先写redoLog
            redoLog.save();
            Object[] a;
            for (int i = 0, size = logs.size(); i < size; i++) {
                a = logs.get(i);
                if (a.length == 3) {
                    ((StorageMap<Object, VersionedValue>) a[0]).remove(a[1]);
                    undoLog.remove((Long) a[2]);
                } else {
                    ((StorageMap<Object, VersionedValue>) a[0]).put(a[1], (VersionedValue) a[2]);
                    undoLog.remove((Long) a[3]);
                }
            }
        }
    }

    /**
     * End this transaction
     *
     * @param t the transaction
     */
    synchronized void endTransaction(MVCCTransaction t) {
        t.setStatus(MVCCTransaction.STATUS_CLOSED);
    }

    /**
     * Rollback to an old savepoint.
     *
     * @param t the transaction
     * @param maxLogId the last log id
     * @param toLogId the log id to roll back to
     */
    void rollbackTo(MVCCTransaction t, long maxLogId, long toLogId) {
        // TODO could synchronize on blocks (100 at a time or so)
        synchronized (undoLog) {
            for (long logId = maxLogId - 1; logId >= toLogId; logId--) {
                Long undoKey = getOperationId(t.transactionId, logId);
                Object[] op = undoLog.get(undoKey);
                if (op == null) {
                    // partially rolled back: load previous
                    undoKey = undoLog.floorKey(undoKey);
                    if (undoKey == null || getTransactionId(undoKey) != t.transactionId) {
                        break;
                    }
                    logId = getLogId(undoKey) + 1;
                    continue;
                }
                int mapId = ((Integer) op[0]).intValue();
                StorageMap<Object, VersionedValue> map = getMap(mapId);
                if (map != null) {
                    Object key = op[1];
                    VersionedValue oldValue = (VersionedValue) op[2];
                    if (oldValue == null) {
                        // this transaction added the value
                        map.remove(key);
                    } else {
                        // this transaction updated the value
                        map.put(key, oldValue);
                    }
                }
                undoLog.remove(undoKey);
            }
        }
    }

    void commitTransactionStatusTable(MVCCTransaction t, String allLocalTransactionNames) {
        t.setCommitTimestamp(nextOddTransactionId());
        TransactionStatusTable.commit(t, allLocalTransactionNames);
        TransactionValidator.enqueue(t, allLocalTransactionNames);
    }

    boolean validateTransaction(int tid, MVCCTransaction currentTransaction) {
        return TransactionStatusTable.validateTransaction(hostAndPort, tid, currentTransaction);
    }

    @Override
    public boolean validateTransaction(String localTransactionName) {
        return TransactionStatusTable.validateTransaction(localTransactionName);
    }

    /**
     * Create a temporary map. Such maps are removed when opening the store.
     *
     * @return the map
     */
    synchronized StorageMap<Object, Integer> createTempMap() {
        String mapName = "temp." + nextTempMapId++;
        return openTempMap(mapName);
    }

    /**
     * Open a temporary map.
     *
     * @param mapName the map name
     * @return the map
     */
    StorageMap<Object, Integer> openTempMap(String mapName) {
        return logStorage.openLogMap(mapName, null, null);
    }

    /**
     * Get the changes of the given transaction, starting from the latest log id
     * back to the given log id.
     *
     * @param t the transaction
     * @param maxLogId the maximum log id
     * @param toLogId the minimum log id
     * @return the changes
     */
    // TODO 目前未使用，H2里面的作用主要是通知触发器
    Iterator<Change> getChanges(final MVCCTransaction t, final long maxLogId, final long toLogId) {
        return new Iterator<Change>() {

            private long logId = maxLogId - 1;
            private Change current;

            {
                fetchNext();
            }

            private void fetchNext() {
                synchronized (undoLog) {
                    while (logId >= toLogId) {
                        Long undoKey = getOperationId(t.transactionId, logId);
                        Object[] op = undoLog.get(undoKey);
                        logId--;
                        if (op == null) {
                            // partially rolled back: load previous
                            undoKey = undoLog.floorKey(undoKey);
                            if (undoKey == null || getTransactionId(undoKey) != t.transactionId) {
                                break;
                            }
                            logId = getLogId(undoKey);
                            continue;
                        }
                        int mapId = ((Integer) op[0]).intValue();
                        StorageMap<Object, VersionedValue> m = getMap(mapId);
                        if (m == null) {
                            // map was removed later on
                        } else {
                            current = new Change();
                            current.mapName = m.getName();
                            current.key = op[1];
                            VersionedValue oldValue = (VersionedValue) op[2];
                            current.value = oldValue == null ? null : oldValue.value;
                            return;
                        }
                    }
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Change next() {
                if (current == null) {
                    throw DataUtils.newUnsupportedOperationException("no data");
                }
                Change result = current;
                fetchNext();
                return result;
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }

}
