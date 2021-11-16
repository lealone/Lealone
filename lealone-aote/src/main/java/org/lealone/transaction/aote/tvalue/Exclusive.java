/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tvalue;

import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.aote.AMTransaction;

class Exclusive extends Uncommitted {

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
            committed = new CommittedWithTid(transaction, value, oldValue.getOldValue(),
                    transaction.getTransactionId());
        } else {
            // 去掉旧版本
            if (oldValue != null && oldValue.isCommitted()
                    && !transaction.getTransactionEngine().containsRepeatableReadTransactions(tid)) {
                oldValue.setOldValue(null);
            }
            committed = new CommittedWithTid(transaction, value, oldValue, transaction.getTransactionId());
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
