/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tvalue;

import org.lealone.db.DataBuffer;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.aote.AMTransaction;

// 用于支持REPEATABLE_READ，小于tid的事务只能读取oldValue
class CommittedWithTid extends TValueBase {

    final long version;
    private final AMTransaction transaction;
    private TransactionalValue oldValue;

    CommittedWithTid(AMTransaction transaction, Object value, TransactionalValue oldValue) {
        this(transaction, value, oldValue, transaction.getTransactionEngine().nextEvenTransactionId());
    }

    CommittedWithTid(AMTransaction transaction, Object value, TransactionalValue oldValue, long versio) {
        super(value);
        this.transaction = transaction;
        this.oldValue = oldValue;
        this.version = versio;
    }

    @Override
    public long getTid() {
        return transaction.getTransactionId();
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
                if (transaction.getTransactionId() >= version)
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
        StringBuilder buff = new StringBuilder();
        buff.append(isCommitted() ? "CommittedWithTid[ " : "Committing[");
        buff.append("tid = ").append(transaction.getTransactionId());
        buff.append(", version = ").append(version);
        buff.append(", value = ").append(value);
        buff.append(", oldValue = ").append(oldValue).append(" ]");
        return buff.toString();
    }
}
