/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tvalue;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.net.NetNode;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.aote.AMTransaction;

class Uncommitted extends TValueBase {

    AMTransaction transaction;
    private final long tid;
    private final int logId;
    TransactionalValue oldValue;
    private final StorageDataType oldValueType;
    private final String hostAndPort;
    // 每次修改记录的事务名要全局唯一，
    // 比如用节点的IP拼接一个本地递增的计数器组成字符串就足够了
    private final String globalReplicationName;
    private long version; // 每次更新时自动加1
    private boolean replicated;
    private boolean rowLock;
    private BitSet lockedColumns;
    private int[] columnIndexes;
    private List<String> retryReplicationNames;

    TransactionalValue ref;

    Uncommitted(AMTransaction transaction, Object value, TransactionalValue oldValue, StorageDataType oldValueType,
            int[] columnIndexes, TransactionalValue ref) {
        super(value);
        // 虽然同一个事务对同一行记录不断更新会导致过长的oldValue链，
        // 但是为了实现保存点的功能还是得这么做，直到事务提交时再取最新值
        // if (oldValue != null && oldValue.getTid() == transaction.getTransactionId()) {
        // oldValue = oldValue.getOldValue();
        // }
        this.transaction = transaction;
        this.tid = transaction.getTransactionId();
        this.logId = transaction.getUndoLog().getLogId();
        this.oldValue = oldValue;
        this.oldValueType = oldValueType;
        this.hostAndPort = NetNode.getLocalTcpHostAndPort();
        this.globalReplicationName = transaction.getGlobalReplicationName();
        this.columnIndexes = columnIndexes;
        this.ref = ref;

        if (columnIndexes == null || columnIndexes.length == 0) {
            rowLock = true;
        } else {
            int columnCount = oldValueType.getColumnCount();
            if (columnIndexes.length < (columnCount / 2) + 1) {
                rowLock = false;
                lockedColumns = new BitSet(columnCount);
                for (int i : columnIndexes) {
                    lockedColumns.set(i);
                }
            } else {
                rowLock = true;
            }
        }
    }

    Uncommitted(long tid, Object value, int logId, TransactionalValue oldValue, StorageDataType oldValueType,
            String hostAndPort, String globalTransactionName, long version) {
        super(value);
        this.tid = tid;
        this.logId = logId;
        this.oldValue = oldValue;
        this.oldValueType = oldValueType;
        this.hostAndPort = hostAndPort;
        this.globalReplicationName = globalTransactionName;
        this.version = version;
    }

    public Uncommitted copy() {
        Uncommitted u = new Uncommitted(tid, value, logId, oldValue, oldValueType, hostAndPort, globalReplicationName,
                version);
        u.replicated = replicated;
        u.rowLock = rowLock;
        u.lockedColumns = lockedColumns;
        u.columnIndexes = columnIndexes;
        u.transaction = transaction;
        return u;
    }

    @Override
    public TransactionalValue getOldValue() {
        return oldValue;
    }

    @Override
    public void setOldValue(TransactionalValue oldValue) {
        this.oldValue = oldValue;
    }

    @Override
    public TransactionalValue getRef() {
        return ref;
    }

    @Override
    public long getTid() {
        return tid;
    }

    @Override
    public long getLockOwnerTid(long tid, int[] columnIndexes) {
        // 1. 当前事务
        // ----------------------------
        if (this.tid == tid) {
            if (oldValue == null)
                return -1;
            else
                return oldValue.getLockOwnerTid(tid, columnIndexes); // 递归检查是否存在锁冲突
        }

        // 2. 不是当前事务
        // ----------------------------
        // 之前的事务已经加了行锁或之前的事务没加行锁，但是当前事务想要进行行锁时，都要拒绝当前事务的锁请求
        if (rowLock || columnIndexes == null)
            return this.tid;
        // 如果当前事务跟之前的事务存在冲突的列锁，那么拒绝当前事务的锁请求
        for (int i : columnIndexes) {
            if (lockedColumns.get(i))
                return this.tid;
        }
        // 递归检查是否存在锁冲突
        if (oldValue != null && oldValue.isLocked(tid, columnIndexes)) {
            return this.tid;
        }
        return -1;
    }

    @Override
    public boolean isLocked(long tid, int[] columnIndexes) {
        // 1. 当前事务
        // ----------------------------
        if (this.tid == tid) {
            if (oldValue == null)
                return false;
            else
                return oldValue.isLocked(tid, columnIndexes); // 递归检查是否存在锁冲突
        }

        // 2. 不是当前事务
        // ----------------------------
        // 之前的事务已经加了行锁或之前的事务没加行锁，但是当前事务想要进行行锁时，都要拒绝当前事务的锁请求
        if (rowLock || columnIndexes == null)
            return true;
        // 如果当前事务跟之前的事务存在冲突的列锁，那么拒绝当前事务的锁请求
        for (int i : columnIndexes) {
            if (lockedColumns.get(i))
                return true;
        }
        // 递归检查是否存在锁冲突
        if (oldValue != null && oldValue.isLocked(tid, columnIndexes)) {
            return true;
        }
        return false;
    }

    @Override
    public int getLogId() {
        return logId;
    }

    @Override
    public String getHostAndPort() {
        return hostAndPort;
    }

    @Override
    public String getGlobalReplicationName() {
        return globalReplicationName;
    }

    @Override
    public boolean isReplicated() {
        return replicated;
    }

    @Override
    public void setReplicated(boolean replicated) {
        this.replicated = replicated;
    }

    @Override
    public List<String> getRetryReplicationNames() {
        return retryReplicationNames;
    }

    @Override
    public void setRetryReplicationNames(List<String> retryReplicationNames) {
        this.retryReplicationNames = retryReplicationNames;
    }

    @Override
    public void incrementVersion() {
        version++;
    }

    @Override
    public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key) {
        if (oldValue == null) { // insert
            map.remove(key);
        } else {
            map.put(key, oldValue); // update或delete
        }
        if (oldValue != null) {
            return oldValue.undo(map, key);
        }
        return oldValue;
    }

    @Override
    public TransactionalValue getCommitted() {
        if (oldValue != null) {
            return oldValue.getCommitted();
        }
        return oldValue;
    }

    @Override
    public TransactionalValue getCommitted(AMTransaction transaction) {
        if (transaction.getTransactionId() == tid || transaction.getIsolationLevel() == Transaction.IL_READ_UNCOMMITTED)
            return this;
        if (oldValue != null) {
            return oldValue.getCommitted(transaction);
        }
        return null;
    }

    @Override
    public boolean isCommitted() {
        return transaction != null && transaction.isCommitted();
    }

    @Override
    public TransactionalValue commit(long tid) {
        boolean noUncommitted = true;
        long minVersion = Long.MAX_VALUE;
        while (true) {
            TransactionalValue first = ref.getRefValue();
            CommittedWithTid committed = new CommittedWithTid(transaction, value, oldValue);
            TransactionalValue next = first;
            TransactionalValue last = committed;
            while (next != null) {
                if (next instanceof CommittedWithTid) {
                    long v = ((CommittedWithTid) next).version;
                    if (v < minVersion)
                        minVersion = v;
                }
                if (next.getTid() == tid && (next.getLogId() == logId || next.isCommitted())) {
                    next = next.getOldValue();
                    continue;
                }
                if (next instanceof Uncommitted && next.getTid() != tid) {
                    Uncommitted u = (Uncommitted) next;
                    u = u.copy(); // 避免多线程执行时修改原来的链接结构
                    if (u.value != null)
                        u.oldValueType.setColumns(u.value, value, columnIndexes);
                    last.setOldValue(u);
                    last = u;
                    noUncommitted = false;
                } else {
                    last.setOldValue(next);
                    last = next;
                }
                next = next.getOldValue();
            }
            last.setOldValue(next);
            if (ref.compareAndSet(first, committed)) {
                // 及时清除不必要的OldValue链
                if (noUncommitted
                        && !transaction.getTransactionEngine().containsRepeatableReadTransactions(minVersion)) {
                    committed.setOldValue(null);
                } else {
                    boolean allCommitted = true;
                    next = committed;
                    while ((next = next.getOldValue()) != null) {
                        if (!next.isCommitted()) {
                            allCommitted = false;
                            break;
                        }
                    }
                    if (allCommitted) {
                        committed.setOldValue(null);
                    }
                }
                break;
            }
        }
        return ref;
    }

    @Override
    public void rollback() {
        TransactionalValue first = ref.getRefValue();
        if (this == first) {
            // 在这里也有可能发生其他事务改变head的情况
            if (ref.compareAndSet(first, this.getOldValue())) {
                return;
            }
        }
        while (true) {
            first = ref.getRefValue();
            TransactionalValue last = first;
            TransactionalValue next = first.getOldValue();
            while (next != null) {
                // 不能用(next == this)，因为有可能已经被copy一次了
                if (next.getTid() == tid && next.getLogId() == logId) {
                    next = next.getOldValue();
                    break;
                }
                last = next;
                next = next.getOldValue();
            }
            last.setOldValue(next);
            if (ref.compareAndSet(first, first))
                break;
        }
    }

    @Override
    public TransactionalValue remove(long tid) {
        TransactionalValue tv = commit(tid);
        return new Removed(tv);
    }

    static Uncommitted read(long tid, StorageDataType valueType, ByteBuffer buff, StorageDataType oldValueType) {
        return read(tid, valueType, buff, oldValueType, false, 0);
    }

    static Uncommitted readMeta(long tid, StorageDataType valueType, ByteBuffer buff, StorageDataType oldValueType,
            int columnCount) {
        return read(tid, valueType, buff, oldValueType, true, columnCount);
    }

    private static Uncommitted read(long tid, StorageDataType valueType, ByteBuffer buff, StorageDataType oldValueType,
            boolean meta, int columnCount) {
        int logId = DataUtils.readVarInt(buff);
        boolean rowLock = buff.get() == 0;
        BitSet lockedColumns = null;
        if (!rowLock) {
            int len = DataUtils.readVarInt(buff);
            byte[] bytes = new byte[len];
            for (int i = 0; i < len; i++) {
                bytes[i] = buff.get();
            }
            lockedColumns = BitSet.valueOf(bytes);
        }
        TransactionalValue oldValue = null;
        if (buff.get() == 1) {
            oldValue = (TransactionalValue) oldValueType.read(buff);
        }
        String hostAndPort = ValueString.type.read(buff);
        String globalReplicationName = ValueString.type.read(buff);
        long version = DataUtils.readVarLong(buff);
        Object value;
        if (meta)
            value = valueType.readMeta(buff, columnCount);
        else
            value = TransactionalValue.readValue(buff, valueType);
        Uncommitted uncommitted = new Uncommitted(tid, value, logId, oldValue, oldValueType, hostAndPort,
                globalReplicationName, version);
        uncommitted.rowLock = rowLock;
        uncommitted.lockedColumns = lockedColumns;
        return uncommitted;
    }

    @Override
    public void writeMeta(DataBuffer buff) {
        buff.putVarLong(tid);
        buff.putVarInt(logId);
        if (rowLock) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            byte[] bytes = lockedColumns.toByteArray();
            int len = bytes.length;
            buff.putVarInt(len);
            for (int i = 0; i < len; i++) {
                buff.put(bytes[i]);
            }
        }
        if (oldValue == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            oldValueType.write(buff, oldValue);
        }
        ValueString.type.write(buff, hostAndPort);
        ValueString.type.write(buff, globalReplicationName);
        buff.putVarLong(version);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder(this.getClass().getSimpleName() + "[ ");
        buff.append("tid = ").append(tid);
        buff.append(", logId = ").append(logId);
        // buff.append(", version = ").append(version);
        // buff.append(", globalReplicationName = ").append(globalReplicationName);
        buff.append(", value = ").append(value).append(", oldValue = ").append(oldValue).append(" ]");
        return buff.toString();
    }
}
