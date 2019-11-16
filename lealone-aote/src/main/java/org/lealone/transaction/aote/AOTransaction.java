/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.transaction.aote;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lealone.common.exceptions.DbException;
import org.lealone.net.NetNode;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.aote.AMTransaction;
import org.lealone.transaction.aote.TransactionalValue;
import org.lealone.transaction.aote.AOTransactionMap.AOReplicationMap;
import org.lealone.transaction.aote.log.LogSyncService;
import org.lealone.transaction.aote.log.RedoLogRecord;

public class AOTransaction extends AMTransaction {

    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    final AOTransactionEngine transactionEngine;
    private final LogSyncService logSyncService;

    Validator validator;

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
        logSyncService = engine.getLogSyncService();
    }

    @Override
    public void setLocal(boolean local) {
        this.local = local;
    }

    /**
     * 假设有RS1、RS2、RS3，Client启动的一个事务涉及这三个RS, 
     * 第一个接收到Client读写请求的RS即是协调者也是参与者，之后Client的任何读写请求都只会跟协调者打交道，
     * 假设这里的协调者是RS1，当读写由RS1转发到RS2时，RS2在完成读写请求后会把它的本地事务名(可能有多个(嵌套事务)发回来，
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
    public void setValidator(Validator validator) {
        this.validator = validator;
    }

    @Override
    public void addParticipant(Participant participant) {
        if (participants == null)
            participants = new ArrayList<>();
        participants.add(participant);
    }

    @Override
    protected <K, V> AOTransactionMap<K, V> createTransactionMap(StorageMap<K, TransactionalValue> map,
            Map<String, String> parameters) {
        boolean isShardingMode = parameters == null ? false : Boolean.parseBoolean(parameters.get("isShardingMode"));
        if (isShardingMode)
            return new AOReplicationMap<>(this, map);
        else
            return new AOTransactionMap<>(this, map);
    }

    @Override
    public void addSavepoint(String name) {
        super.addSavepoint(name);

        if (participants != null && !isAutoCommit())
            parallelSavepoint(true, name);
    }

    @Override
    public void commit() {
        if (local) {
            commitLocal();
        } else {
            commit(null);
        }
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        if (allLocalTransactionNames == null)
            allLocalTransactionNames = getAllLocalTransactionNames();
        List<Future<Void>> futures = null;
        if (participants != null && !isAutoCommit())
            futures = parallelCommitOrRollback(allLocalTransactionNames);

        commitLocalAndTransactionStatusTable(allLocalTransactionNames);
        if (futures != null)
            waitFutures(futures);
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
        TransactionStatusTable.put(this, allLocalTransactionNames);
        TransactionValidator.enqueue(this, allLocalTransactionNames);
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

    private void waitFutures(List<Future<Void>> futures) {
        try {
            for (int i = 0, size = futures.size(); i < size; i++) {
                futures.get(i).get();
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public boolean isLocal() {
        return transactionId % 2 == 0;
    }

    private List<Future<Void>> parallelCommitOrRollback(final String allLocalTransactionNames) {
        int size = participants.size();
        List<Future<Void>> futures = new ArrayList<>(size);
        for (final Participant participant : participants) {
            futures.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (allLocalTransactionNames != null)
                        participant.commitTransaction(allLocalTransactionNames);
                    else
                        participant.rollbackTransaction();
                    return null;
                }
            }));
        }
        return futures;
    }

    @Override
    public void rollbackToSavepoint(String name) {
        super.rollbackToSavepoint(name);
        if (participants != null && !isAutoCommit())
            parallelSavepoint(false, name);
    }

    private void parallelSavepoint(final boolean add, final String name) {
        int size = participants.size();
        List<Future<Void>> futures = new ArrayList<>(size);
        for (final Participant participant : participants) {
            futures.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (add)
                        participant.addSavepoint(name);
                    else
                        participant.rollbackToSavepoint(name);
                    return null;
                }
            }));
        }
        try {
            for (int i = 0; i < size; i++) {
                futures.get(i).get();
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private String getAllLocalTransactionNames() {
        getLocalTransactionNames();
        return localTransactionNamesBuilder.toString();
    }

}
