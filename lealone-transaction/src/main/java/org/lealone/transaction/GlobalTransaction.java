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
package org.lealone.transaction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lealone.api.ErrorCode;
import org.lealone.engine.FrontendSession;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.util.New;

public class GlobalTransaction extends TransactionBase {
    private static final CommitHashMap commitHashMap = new CommitHashMap(1000, 32);
    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    //private final Session session;

    //参与本次事务的其他FrontendSession
    private final Map<String, FrontendSession> frontendSessionCache = New.hashMap();

    private final String transactionName;
    //    private final long transactionId;
    //    private final boolean autoCommit;
    //    private long commitTimestamp;

    //协调者或参与者自身的本地事务名
    private StringBuilder localTransactionNamesBuilder;
    //如果本事务是协调者中的事务，那么在此字段中存放其他参与者的本地事务名
    private final ConcurrentSkipListSet<String> participantLocalTransactionNames = new ConcurrentSkipListSet<String>();
    //
    //    private CopyOnWriteArrayList<Row> undoRows;
    //    private HashMap<String, Integer> savepoints;
    private final ConcurrentSkipListSet<Long> halfSuccessfulTransactions = new ConcurrentSkipListSet<Long>();

    public GlobalTransaction(Session session) {
        super(session);
        String hostAndPort = session.getHostAndPort();
        if (hostAndPort == null)
            hostAndPort = "localhost:0";
        transactionName = getTransactionName(hostAndPort, transactionId);
    }

    void addFrontendSession(String url, FrontendSession frontendSession) {
        frontendSessionCache.put(url, frontendSession);
    }

    FrontendSession getFrontendSession(String url) {
        return frontendSessionCache.get(url);
    }

    public long getNewTimestamp() {
        if (autoCommit)
            return TimestampServiceTable.nextEven();
        else
            return TimestampServiceTable.nextOdd();
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
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
            for (String name : localTransactionNames.split(","))
                participantLocalTransactionNames.add(name.trim());
        }
    }

    @Override
    public String getLocalTransactionNames() {
        StringBuilder buff = new StringBuilder(transactionName);

        if (!participantLocalTransactionNames.isEmpty()) {
            for (String name : participantLocalTransactionNames) {
                buff.append(',');
                buff.append(name);
            }
        }

        if (localTransactionNamesBuilder != null && localTransactionNamesBuilder.equals(buff))
            return null;
        localTransactionNamesBuilder = buff;
        return buff.toString();
    }

    public String getAllLocalTransactionNames() {
        getLocalTransactionNames();
        return localTransactionNamesBuilder.toString();
    }

    public String getTransactionName() {
        return transactionName;
    }

    @Override
    public String toString() {
        return "T-" + transactionId;
    }

    @Override
    public void commit() {
        commit(null);
    }

    @Override
    public void commit(String allLocalTransactionNames) {
        try {
            if (allLocalTransactionNames == null)
                allLocalTransactionNames = getAllLocalTransactionNames();
            List<Future<Void>> futures = null;
            if (!isAutoCommit() && frontendSessionCache.size() > 0)
                futures = parallelCommitOrRollback(allLocalTransactionNames);

            commit0(allLocalTransactionNames);
            if (futures != null)
                waitFutures(futures);
        } finally {
            endTransaction();
        }

    }

    private void commit0(String allLocalTransactionNames) {
        if (!autoCommit) {
            try {
                //1. 获得提交时间戳
                commitTimestamp = TimestampServiceTable.nextOdd();

                //2. 检测写写冲突
                checkConflict();

                //3. 更新事务状态表
                TransactionStatusTable.commit(this, allLocalTransactionNames);

                //4.缓存本次事务已提交的行，用于下一个事务的写写冲突检测
                cacheCommittedRows();

                //TODO 考虑如何缓存事务id和提交时间戳? 难点是: 当前节点提交了，但是还不能完全确定全局事务正常提交
            } catch (Exception e) {
                rollback();
                throw DbException.convert(e);
            } finally {
                endTransaction();
            }
        }
    }

    private void checkConflict() {
        synchronized (commitHashMap) {
            if (transactionId < TimestampServiceTable.first()) {
                //1. transactionId不可能小于region server启动时从TimestampServiceTable中获得的上一次的最大时间戳
                throw DbException.throwInternalError("transactionId(" + transactionId + ") < firstTimestampService("
                        + TimestampServiceTable.first() + ")");
            } else if (!undoRows.isEmpty() && transactionId < commitHashMap.getLargestDeletedTimestamp()) {
                //2. Too old and not read only
                throw new RuntimeException("Too old startTimestamp: ST " + transactionId + " MAX "
                        + commitHashMap.getLargestDeletedTimestamp());
            } else {
                //3. write-write冲突检测
                for (Row row : undoRows) {
                    long oldCommitTimestamp = commitHashMap.getLatestWriteForRow(row.hashCode());
                    if (oldCommitTimestamp != 0 && oldCommitTimestamp > transactionId) {
                        throw new RuntimeException("Write-write conflict: oldCommitTimestamp " + oldCommitTimestamp
                                + ", startTimestamp " + transactionId + ", rowKey " + row.getRowKey());
                    }
                }

                //4.检查此事务关联到的一些半成功事务是否全成功了
                if (!halfSuccessfulTransactions.isEmpty()) {
                    String hostAndPort = session.getHostAndPort();
                    for (Long tid : halfSuccessfulTransactions) {
                        if (!TransactionStatusTable.isFullSuccessful(hostAndPort, tid)) {
                            throw new RuntimeException("Write-write conflict: transaction "
                                    + getTransactionName(hostAndPort, tid) + " is not full successful, current transaction: "
                                    + transactionName);
                        }
                    }
                }

            }
        }
    }

    private void cacheCommittedRows() {
        synchronized (commitHashMap) {
            //不能把下面的代码放入第3步的for循环中，只有冲突检测完后才能put提交记录
            for (Row row : undoRows) {
                commitHashMap.putLatestWriteForRow(row.hashCode(), getCommitTimestamp());
            }
        }
    }

    @Override
    public void rollback() {
        if (!autoCommit) {
            try {
                undo();
            } catch (Exception e) {
                throw DbException.convert(e);
            } finally {
                endTransaction();
            }
        }
    }

    private void undo() {
        if (undoRows != null) {
            for (int i = undoRows.size() - 1; i >= 0; i--) {
                Row row = undoRows.get(i);
                row.getTable().removeRow(session, row, true);
            }
        }
    }

    public void log(Row row) {
        if (!autoCommit)
            undoRows.add(row);
    }

    public void addSavepoint(String name) {
        if (savepoints == null)
            savepoints = session.getDatabase().newStringMap();

        savepoints.put(name, undoRows.size());
    }

    @Override
    public void rollbackToSavepoint(String name) {
        if (savepoints == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }

        Integer savepointIndex = savepoints.get(name);
        if (savepointIndex == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1, name);
        }
        int i = savepointIndex.intValue();
        int size;
        Row row;
        while ((size = undoRows.size()) > i) {
            row = undoRows.remove(size - 1);
            row.getTable().removeRow(session, row, true);
        }
        if (savepoints != null) {
            String[] names = new String[savepoints.size()];
            savepoints.keySet().toArray(names);
            for (String n : names) {
                savepointIndex = savepoints.get(n);
                if (savepointIndex.intValue() >= i) {
                    savepoints.remove(n);
                }
            }
        }
    }

    private void endTransaction() {
        if (undoRows != null) {
            undoRows.clear();
            undoRows = null;
        }

        if (!frontendSessionCache.isEmpty()) {
            for (FrontendSession fs : frontendSessionCache.values()) {
                fs.setTransaction(null);
                //TODO
                //FrontendSessionPool.release(fs);
            }

            frontendSessionCache.clear();
        }

        if (!session.isRoot())
            session.setAutoCommit(true);
    }

    private List<Future<Void>> parallelCommitOrRollback(final String allLocalTransactionNames) {
        int size = frontendSessionCache.size();
        List<Future<Void>> futures = New.arrayList(size);
        for (final FrontendSession fs : frontendSessionCache.values()) {
            futures.add(executorService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (allLocalTransactionNames != null)
                        fs.commitTransaction(allLocalTransactionNames);
                    else
                        fs.rollbackTransaction();
                    return null;
                }
            }));
        }
        return futures;
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

    public void addHalfSuccessfulTransaction(Long tid) {
        halfSuccessfulTransactions.add(tid);
    }

    public static String getTransactionName(String hostAndPort, long tid) {
        StringBuilder buff = new StringBuilder(hostAndPort);
        buff.append(':');
        buff.append(tid);
        return buff.toString();
    }
}
