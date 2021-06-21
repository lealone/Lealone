/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;

import org.lealone.db.RunMode;
import org.lealone.net.NetNode;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.aote.log.RedoLogRecord;

public class AOTransaction extends AMTransaction {

    final AOTransactionEngine transactionEngine;

    private boolean local = true; // 默认是true，如果是分布式事务才设为false

    // 协调者或参与者自身的本地事务名
    private StringBuilder localTransactionNamesBuilder;
    // 如果本事务是协调者中的事务，那么在此字段中存放其他参与者的本地事务名
    private ConcurrentSkipListSet<String> participantLocalTransactionNames;
    private List<Participant> participants;

    private long commitTimestamp;

    long getCommitTimestamp() {
        return commitTimestamp;
    }

    AOTransaction(AOTransactionEngine engine, long tid) {
        super(engine, tid, NetNode.getLocalTcpHostAndPort());
        transactionEngine = engine;
    }

    @Override
    public void setLocal(boolean local) {
        this.local = local;
    }

    /**
     * 假设有node1、node2、node3三个节点，Client启动的一个事务涉及这三个node, 
     * 第一个接收到Client读写请求的node即是协调者也是参与者，之后Client的任何读写请求都只会跟协调者打交道，
     * 假设这里的协调者是node1，当读写由node1转发到node2时，node2在完成读写请求后会把它的本地事务名(可能有多个(嵌套事务)发回来，
     * 此时协调者必须记下所有其他参与者的本地事务名。<p>
     * 
     * 如果本地事务名是null，代表参与者执行完读写请求后发现跟上次的本地事务名一样，为了减少网络传输就不再重发。
     */
    @Override
    public void addLocalTransactionNames(String localTransactionNames) {
        if (localTransactionNames != null) {
            if (participantLocalTransactionNames == null)
                participantLocalTransactionNames = new ConcurrentSkipListSet<>();
            for (String name : localTransactionNames.split(","))
                participantLocalTransactionNames.add(name.trim());
        }
    }

    @Override
    public String getLocalTransactionNames() {
        StringBuilder buff = new StringBuilder(transactionName);

        if (participantLocalTransactionNames != null) {
            for (String name : participantLocalTransactionNames) {
                buff.append(',');
                buff.append(name);
            }
        }
        // 两个StringBuilder对象不能用equals比较
        if (localTransactionNamesBuilder != null && localTransactionNamesBuilder.toString().equals(buff.toString()))
            return null;
        localTransactionNamesBuilder = buff;
        return buff.toString();
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
        super.asyncCommit(asyncTask);
        // commit();
        // if (asyncTask != null) {
        // try {
        // asyncTask.run();
        // } catch (Exception e) {
        // throw DbException.convert(e);
        // }
        // }
    }

    @Override
    public void commit() {
        if (local) {
            commitLocal();
            if (globalReplicationName != null)
                DTRValidator.removeReplication(globalReplicationName);
        } else {
            commit(null);
        }
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        if (allLocalTransactionNames == null)
            allLocalTransactionNames = getAllLocalTransactionNames();
        if (participants != null && !isAutoCommit()) {
            for (Participant participant : participants) {
                participant.commitTransaction(allLocalTransactionNames);
            }
        }
        commitLocalAndTransactionStatusTable(allLocalTransactionNames);
    }

    private void commitLocalAndTransactionStatusTable(String allLocalTransactionNames) {
        checkNotClosed();

        commitTimestamp = transactionEngine.nextOddTransactionId();
        RedoLogRecord r = createDistributedTransactionRedoLogRecord(allLocalTransactionNames);
        if (r != null) { // 事务没有进行任何操作时不用同步日志
            // 先写redoLog
            logSyncService.addAndMaybeWaitForSync(r);
        }
        // 分布式事务推迟提交
        if (isLocal()) {
            commitFinal();
        }
        DTRValidator.addTransaction(this, allLocalTransactionNames);
    }

    private RedoLogRecord createDistributedTransactionRedoLogRecord(String allLocalTransactionNames) {
        ByteBuffer operations = getUndoLog().toRedoLogRecordBuffer(transactionEngine);
        if (operations == null)
            return null;
        return RedoLogRecord.createDistributedTransactionRedoLogRecord(transactionId, transactionName,
                allLocalTransactionNames, commitTimestamp, operations);
    }

    void commitAfterValidate(long tid) {
        commitFinal(tid);
    }

    @Override
    public boolean isLocal() {
        return transactionId % 2 == 0;
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

    private String getAllLocalTransactionNames() {
        getLocalTransactionNames();
        return localTransactionNamesBuilder.toString();
    }
}
