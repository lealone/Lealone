/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.net.NetNode;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.Transaction;

public interface TransactionalValue {

    // 对于一个已经提交的值，如果当前事务因为隔离级别的原因读不到这个值，那么就返回SIGHTLESS
    public static final TransactionalValue SIGHTLESS = TransactionalValue.createCommitted(null);

    public Object getValue();

    public TransactionalValue getOldValue();

    public void setOldValue(TransactionalValue oldValue);

    public TransactionalValue getRef();

    public TransactionalValue getRefValue();

    public void setRefValue(TransactionalValue v);

    public boolean compareAndSet(TransactionalValue expect, TransactionalValue update);

    // 如果是0代表事务已经提交，对于已提交事务，只有在写入时才写入tid=0，
    // 读出来的时候为了不占用内存就不加tid字段了，这样每条已提交记录能省8个字节(long)的内存空间
    public long getTid();

    public int getLogId();

    public boolean isLocked(long tid, int[] columnIndexes);

    public long getLockOwnerTid(long tid, int[] columnIndexes);

    public String getHostAndPort();

    public String getGlobalReplicationName();

    public boolean isReplicated();

    public void setReplicated(boolean replicated);

    public default List<String> getRetryReplicationNames() {
        return null;
    }

    public default void setRetryReplicationNames(List<String> retryReplicationNames) {
    }

    public void incrementVersion();

    public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key);

    public TransactionalValue getCommitted();

    public TransactionalValue getCommitted(AMTransaction transaction);

    public TransactionalValue commit(long tid);

    public void rollback();

    public TransactionalValue remove(long tid);

    public boolean isCommitted();

    public void write(DataBuffer buff, StorageDataType valueType);

    public void writeMeta(DataBuffer buff);

    public void writeValue(DataBuffer buff, StorageDataType valueType);

    public static TransactionalValue readMeta(ByteBuffer buff, StorageDataType valueType, StorageDataType oldValueType,
            int columnCount) {
        long tid = DataUtils.readVarLong(buff);
        if (tid == 0) {
            Object value = valueType.readMeta(buff, columnCount);
            return TransactionalValue.createCommitted(value);
        } else {
            return Uncommitted.readMeta(tid, valueType, buff, oldValueType, columnCount);
        }
    }

    public static Object readValue(ByteBuffer buff, StorageDataType valueType) {
        Object value = null;
        if (buff.get() == 1) {
            value = valueType.read(buff);
        }
        return value;
    }

    public static TransactionalValue read(ByteBuffer buff, StorageDataType valueType, StorageDataType oldValueType) {
        long tid = DataUtils.readVarLong(buff);
        if (tid == 0) {
            Object value = TransactionalValue.readValue(buff, valueType);
            // 需要返回引用，否则无法在修改和删除时使用CAS
            return createRef(TransactionalValue.createCommitted(value));
        } else {
            return createRef(Uncommitted.read(tid, valueType, buff, oldValueType));
        }
    }

    public static TransactionalValue createUncommitted(AMTransaction transaction, Object value,
            TransactionalValue oldValue, StorageDataType oldValueType, int[] columnIndexes, TransactionalValue ref) {
        boolean rowLock = false;
        if (oldValue == null || oldValue instanceof Exclusive) { // insert
            rowLock = true;
        } else if (value == null) { // delete
            rowLock = true;
        } else {
            if (columnIndexes == null || columnIndexes.length == 0) {
                rowLock = true;
            } else {
                int columnCount = oldValueType.getColumnCount();
                if (columnIndexes.length >= (columnCount / 2) + 1) {
                    rowLock = true;
                }
            }
        }
        if (rowLock)
            return new Exclusive(transaction, value, oldValue, oldValueType, columnIndexes, ref);
        else
            return new Uncommitted(transaction, value, oldValue, oldValueType, columnIndexes, ref);
    }

    public static TransactionalValue createRef(TransactionalValue tv) {
        return new TransactionalValueRef(tv);
    }

    public static TransactionalValue createRef() {
        return new TransactionalValueRef(null);
    }

    public static TransactionalValue createCommitted(Object value) {
        return new Committed(value);
    }

    // 因为每条记录都对应此类的一个实例，所以为了节约内存没有直接使用java.util.concurrent.atomic.AtomicReference
    public static class TransactionalValueRef implements TransactionalValue {

        private static final AtomicReferenceFieldUpdater<TransactionalValueRef, TransactionalValue> tvUpdater = AtomicReferenceFieldUpdater
                .newUpdater(TransactionalValueRef.class, TransactionalValue.class, "tv");

        private volatile TransactionalValue tv;
        private List<String> retryReplicationNames;

        TransactionalValueRef(TransactionalValue transactionalValue) {
            tv = transactionalValue;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("Ref[ ");
            buff.append(tv).append(" ]");
            return buff.toString();
        }

        @Override
        public Object getValue() {
            return tv.getValue();
        }

        @Override
        public TransactionalValue getOldValue() {
            return tv.getOldValue();
        }

        @Override
        public void setOldValue(TransactionalValue oldValue) {
            tv.setOldValue(oldValue);
        }

        @Override
        public boolean compareAndSet(TransactionalValue expect, TransactionalValue update) {
            return tvUpdater.compareAndSet(this, expect, update);
        }

        @Override
        public TransactionalValue getRef() {
            return this;
        }

        @Override
        public TransactionalValue getRefValue() {
            return tv;
        }

        @Override
        public void setRefValue(TransactionalValue v) {
            tv = v;
        }

        @Override
        public long getTid() {
            return tv.getTid();
        }

        @Override
        public int getLogId() {
            return tv.getLogId();
        }

        @Override
        public boolean isLocked(long tid, int[] columnIndexes) {
            return tv.isLocked(tid, columnIndexes);
        }

        @Override
        public long getLockOwnerTid(long tid, int[] columnIndexes) {
            return tv.getLockOwnerTid(tid, columnIndexes);
        }

        @Override
        public String getHostAndPort() {
            return tv.getHostAndPort();
        }

        @Override
        public String getGlobalReplicationName() {
            return tv.getGlobalReplicationName();
        }

        @Override
        public boolean isReplicated() {
            return tv.isReplicated();
        }

        @Override
        public void setReplicated(boolean replicated) {
            tv.setReplicated(replicated);
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
            tv.incrementVersion();
        }

        @Override
        public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key) {
            return tv.undo(map, key);
        }

        @Override
        public TransactionalValue getCommitted() {
            return tv.getCommitted();
        }

        @Override
        public TransactionalValue getCommitted(AMTransaction transaction) {
            return tv.getCommitted(transaction);
        }

        @Override
        public TransactionalValue commit(long tid) {
            return tv.commit(tid);
        }

        @Override
        public void rollback() {
            tv.rollback();
        }

        @Override
        public TransactionalValue remove(long tid) {
            return tv.remove(tid);
        }

        @Override
        public boolean isCommitted() {
            return tv == null || tv.isCommitted();
        }

        @Override
        public void write(DataBuffer buff, StorageDataType valueType) {
            tv.write(buff, valueType);
        }

        @Override
        public void writeMeta(DataBuffer buff) {
            tv.writeMeta(buff);
        }

        @Override
        public void writeValue(DataBuffer buff, StorageDataType valueType) {
            tv.writeValue(buff, valueType);
        }
    }

    static abstract class TransactionalValueBase implements TransactionalValue {

        public final Object value;

        public TransactionalValueBase(Object value) {
            this.value = value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public boolean compareAndSet(TransactionalValue expect, TransactionalValue update) {
            return true;
        }

        @Override
        public TransactionalValue getRef() {
            return null;
        }

        @Override
        public TransactionalValue getRefValue() {
            return this;
        }

        @Override
        public void setRefValue(TransactionalValue v) {
        }

        // 如果是0代表事务已经提交，对于已提交事务，只有在写入时才写入tid=0，
        // 读出来的时候为了不占用内存就不加tid字段了，这样每条已提交记录能省8个字节(long)的内存空间
        @Override
        public long getTid() {
            return 0;
        }

        @Override
        public int getLogId() {
            return 0;
        }

        @Override
        public boolean isLocked(long tid, int[] columnIndexes) {
            return false;
        }

        @Override
        public long getLockOwnerTid(long tid, int[] columnIndexes) {
            return -1;
        }

        @Override
        public String getHostAndPort() {
            return null;
        }

        @Override
        public String getGlobalReplicationName() {
            return null;
        }

        @Override
        public boolean isReplicated() {
            return false;
        }

        @Override
        public void setReplicated(boolean replicated) {
        }

        @Override
        public void incrementVersion() {
        }

        @Override
        public <K> TransactionalValue undo(StorageMap<K, TransactionalValue> map, K key) {
            return this;
        }

        @Override
        public TransactionalValue getCommitted() {
            return this;
        }

        @Override
        public TransactionalValue getCommitted(AMTransaction transaction) {
            return this;
        }

        @Override
        public TransactionalValue commit(long tid) {
            return this;
        }

        @Override
        public boolean isCommitted() {
            return true;
        }

        @Override
        public void rollback() {
        }

        @Override
        public TransactionalValue remove(long tid) {
            return this;
        }

        @Override
        public void write(DataBuffer buff, StorageDataType valueType) {
            writeMeta(buff);
            writeValue(buff, valueType);
        }

        @Override
        public abstract void writeMeta(DataBuffer buff);

        @Override
        public void writeValue(DataBuffer buff, StorageDataType valueType) {
            if (value == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                valueType.write(buff, value);
            }
        }
    }

    static class Committed extends TransactionalValueBase {
        Committed(Object value) {
            super(value);
        }

        @Override
        public TransactionalValue getOldValue() {
            return null;
        }

        @Override
        public void setOldValue(TransactionalValue oldValue) {
        }

        @Override
        public void writeMeta(DataBuffer buff) {
            buff.putVarLong(0);
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("Committed[ ");
            buff.append(value).append(" ]");
            return buff.toString();
        }
    }

    // 用于支持REPEATABLE_READ，小于tid的事务只能读取oldValue
    static class CommittedWithTid extends TransactionalValueBase {
        final long version;
        private final AMTransaction transaction;
        private TransactionalValue oldValue;

        CommittedWithTid(AMTransaction transaction, Object value, TransactionalValue oldValue) {
            this(transaction, value, oldValue, transaction.transactionEngine.nextEvenTransactionId());
        }

        CommittedWithTid(AMTransaction transaction, Object value, TransactionalValue oldValue, long versio) {
            super(value);
            this.transaction = transaction;
            this.oldValue = oldValue;
            this.version = versio;
        }

        @Override
        public long getTid() {
            return transaction.transactionId;
        }

        @Override
        public boolean isLocked(long tid, int[] columnIndexes) {
            // 列锁的场景，oldValue可能是未提交的，所以要进一步判断
            return oldValue != null && oldValue.isLocked(tid, columnIndexes);
        }

        @Override
        public long getLockOwnerTid(long tid, int[] columnIndexes) {
            if (oldValue == null)
                return -1;
            else
                return oldValue.getLockOwnerTid(tid, columnIndexes);
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
        public TransactionalValue getCommitted(AMTransaction transaction) {
            if (this.transaction.isCommitted()) {
                switch (transaction.getIsolationLevel()) {
                case Transaction.IL_REPEATABLE_READ:
                case Transaction.IL_SERIALIZABLE:
                    if (transaction.transactionId >= version)
                        return this;
                    else if (oldValue != null) {
                        return oldValue.getCommitted(transaction);
                    }
                    return SIGHTLESS;
                default:
                    return this;
                }
            } else {
                if (transaction.getIsolationLevel() == Transaction.IL_READ_UNCOMMITTED) {
                    return this;
                } else if (oldValue != null) {
                    return oldValue.getCommitted(transaction);
                } else {
                    return null;
                }
            }
        }

        @Override
        public void writeMeta(DataBuffer buff) {
            buff.putVarLong(0);
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder(transaction.isCommitted() ? "CommittedWithTid[ " : "Committing[");
            buff.append("tid = ").append(transaction.transactionId).append(", version = ").append(version);
            buff.append(", value = ").append(value).append(", oldValue = ").append(oldValue).append(" ]");
            return buff.toString();
        }
    }

    static class Removed extends TransactionalValueBase {
        private TransactionalValue oldValue;

        Removed(TransactionalValue oldValue) {
            super(oldValue.getValue());
            this.oldValue = oldValue;
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
        public void writeMeta(DataBuffer buff) {
            buff.putVarLong(0);
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder("Removed[ ");
            buff.append(value).append(" ]");
            return buff.toString();
        }
    }

    static class Uncommitted extends TransactionalValueBase {

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
            // if (oldValue != null && oldValue.getTid() == transaction.transactionId) {
            // oldValue = oldValue.getOldValue();
            // }
            this.transaction = transaction;
            this.tid = transaction.transactionId;
            this.logId = transaction.undoLog.getLogId();
            this.oldValue = oldValue;
            this.oldValueType = oldValueType;
            this.hostAndPort = NetNode.getLocalTcpHostAndPort();
            this.globalReplicationName = transaction.globalReplicationName;
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
            Uncommitted u = new Uncommitted(tid, value, logId, oldValue, oldValueType, hostAndPort,
                    globalReplicationName, version);
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
            if (transaction.transactionId == tid || transaction.getIsolationLevel() == Transaction.IL_READ_UNCOMMITTED)
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
                            && !transaction.transactionEngine.containsRepeatableReadTransactions(minVersion)) {
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

        private static Uncommitted read(long tid, StorageDataType valueType, ByteBuffer buff,
                StorageDataType oldValueType) {
            return read(tid, valueType, buff, oldValueType, false, 0);
        }

        private static Uncommitted readMeta(long tid, StorageDataType valueType, ByteBuffer buff,
                StorageDataType oldValueType, int columnCount) {
            return read(tid, valueType, buff, oldValueType, true, columnCount);
        }

        private static Uncommitted read(long tid, StorageDataType valueType, ByteBuffer buff,
                StorageDataType oldValueType, boolean meta, int columnCount) {
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

    static class Exclusive extends Uncommitted {

        Exclusive(AMTransaction transaction, Object value, TransactionalValue oldValue, StorageDataType oldValueType,
                int[] columnIndexes, TransactionalValue ref) {
            super(transaction, value, oldValue, oldValueType, columnIndexes, ref);
        }

        @Override
        public boolean isLocked(long tid, int[] columnIndexes) {
            return getTid() != tid;
        }

        @Override
        public long getLockOwnerTid(long tid, int[] columnIndexes) {
            return getTid();
        }

        @Override
        public TransactionalValue commit(long tid) {
            CommittedWithTid committed;
            if (oldValue != null && oldValue.getTid() == tid) {
                // 同一个事务对同一个key更新了多次时只保留最近的一次
                committed = new CommittedWithTid(transaction, value, oldValue.getOldValue(), transaction.transactionId);
            } else {
                // 去掉旧版本
                if (oldValue != null && oldValue.isCommitted()
                        && !transaction.transactionEngine.containsRepeatableReadTransactions(tid)) {
                    oldValue.setOldValue(null);
                }
                committed = new CommittedWithTid(transaction, value, oldValue, transaction.transactionId);
            }
            TransactionalValue first = ref.getRefValue();
            if (this == first) {
                ref.setRefValue(committed);
            } else {
                TransactionalValue last = first;
                TransactionalValue next = first.getOldValue();
                while (next != null) {
                    if (next == this) {
                        last.setOldValue(committed);
                        break;
                    }
                    last = next;
                    next = next.getOldValue();
                }
            }
            return ref;
        }

        @Override
        public void rollback() {
            TransactionalValue first = ref.getRefValue();

            // 因为执行rollback时是按最新到最老的顺序进行的，
            // 所以当前被rollback的TransactionalValue一定是RefValue
            // if (this != first)
            // throw DbException.getInternalError();

            // 执行update时先锁后更新，会有两条记录
            if (this != first) {
                TransactionalValue last = first;
                TransactionalValue next = first.getOldValue();
                while (next != null) {
                    if (next == this) {
                        last.setOldValue(next.getOldValue());
                        break;
                    }
                    last = next;
                    next = next.getOldValue();
                }
            } else {
                ref.setRefValue(this.getOldValue());
            }
        }
    }
}
