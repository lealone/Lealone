/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import org.lealone.net.NetNode;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;

//支持分布式场景(包括replication和sharding)
public class AOTransactionMap<K, V> extends AMTransactionMap<K, V> {

    private final AOTransaction transaction;

    AOTransactionMap(AOTransaction transaction, StorageMap<K, TransactionalValue> map) {
        super(transaction, map);
        this.transaction = transaction;
    }

    @Override
    protected TransactionalValue getDistributedValue(K key, TransactionalValue data) {
        // 第一种: 复制的场景
        // 数据从节点A迁移到节点B的过程中，如果把A中未提交的值也移到B中，
        // 那么在节点B中会读到不一致的数据，此时需要从节点A读出正确的值
        // TODO 如何更高效的判断，不用比较字符串
        if (data.getHostAndPort() != null && !data.getHostAndPort().equals(NetNode.getLocalTcpHostAndPort())) {
            return getRemoteTransactionalValue(data.getHostAndPort(), key);
        }
        // 第二种: 分布式场景
        long tid = data.getTid();
        if (tid % 2 == 1) {
            boolean isValid = AOTransactionEngine.validateTransaction(tid, transaction);
            if (isValid) {
                transaction.commitAfterValidate(tid);
                return getValue(key, map.get(key));
            }
        } else if (data.getGlobalReplicationName() != null) {
            if (data.isReplicated())
                return data;
            else if (DTRValidator.containsReplication(data.getGlobalReplicationName())) {
                boolean isValid = DTRValidator.validateReplication(data.getGlobalReplicationName(),
                        transaction.getSession());
                if (isValid) {
                    DTRValidator.removeReplication(data.getGlobalReplicationName());
                    data.setReplicated(true);
                    return data;
                }
            }
        }
        return null;
    }

    // TODO 还未实现
    private TransactionalValue getRemoteTransactionalValue(String hostAndPort, K key) {
        return null;
    }

    @Override
    protected void afterAddComplete() {
        if (transaction.globalReplicationName != null)
            DTRValidator.addReplication(transaction.globalReplicationName);
    }

    @Override
    protected int tryUpdateOrRemove(K key, V value, int[] columnIndexes, TransactionalValue oldTransactionalValue,
            boolean isLockedBySelf) {
        long tid = oldTransactionalValue.getTid();
        if (tid != 0 && tid != transaction.transactionId && tid % 2 == 1) {
            boolean isValid = AOTransactionEngine.validateTransaction(tid, transaction);
            if (isValid) {
                transaction.commitAfterValidate(tid);
            } else {
                return Transaction.OPERATION_NEED_WAIT;
            }
        }
        int ret = super.tryUpdateOrRemove(key, value, columnIndexes, oldTransactionalValue, isLockedBySelf);
        if (ret == Transaction.OPERATION_COMPLETE) {
            oldTransactionalValue.incrementVersion();
            if (transaction.globalReplicationName != null)
                DTRValidator.addReplication(transaction.globalReplicationName);
        }
        return ret;
    }

    @Override
    protected int addWaitingTransaction(Object key, TransactionalValue oldTransactionalValue) {
        return addWaitingTransaction(key, oldTransactionalValue, null);
    }

    @Override
    protected int addWaitingTransaction(Object key, TransactionalValue oldTransactionalValue,
            Transaction.Listener listener) {
        // 老的实现方案
        // if (transaction.globalReplicationName != null) {
        // ByteBuffer keyBuff;
        // try (DataBuffer buff = DataBuffer.create()) {
        // getKeyType().write(buff, key);
        // keyBuff = buff.getAndCopyBuffer();
        // }
        // DTRValidator
        // .handleReplicationConflict(getName(), keyBuff, transaction.globalReplicationName,
        // transaction.getSession(), oldTransactionalValue.getGlobalReplicationName())
        // .onSuccess(candidateReplicationName -> {
        // if (candidateReplicationName.equals(transaction.globalReplicationName)) {
        // AMTransaction old = transaction.transactionEngine
        // .getTransaction(oldTransactionalValue.getTid());
        // if (old != null) {
        // old.getUndoLog().undo();
        // oldTransactionalValue.rollback();
        // old.wakeUpWaitingTransaction(transaction);
        // }
        // }
        // });
        // }
        if (listener != null)
            return super.addWaitingTransaction(key, oldTransactionalValue, listener);
        else
            return super.addWaitingTransaction(key, oldTransactionalValue);
    }

    @Override
    public AMTransactionMap<K, V> getInstance(Transaction transaction) {
        return new AOTransactionMap<>((AOTransaction) transaction, map);
    }
}
