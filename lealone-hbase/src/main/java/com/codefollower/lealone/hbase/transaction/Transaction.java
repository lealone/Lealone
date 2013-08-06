/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.lealone.hbase.transaction;

import static com.codefollower.lealone.hbase.engine.HBaseConstants.DEFAULT_TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY;
import static com.codefollower.lealone.hbase.engine.HBaseConstants.DEFAULT_TRANSACTION_COMMIT_CACHE_SIZE;
import static com.codefollower.lealone.hbase.engine.HBaseConstants.TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY;
import static com.codefollower.lealone.hbase.engine.HBaseConstants.TRANSACTION_COMMIT_CACHE_SIZE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.codefollower.lealone.engine.SessionRemote;
import com.codefollower.lealone.hbase.command.CommandParallel;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.engine.SessionRemotePool;
import com.codefollower.lealone.hbase.metadata.TransactionStatusTable;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.util.New;

public class Transaction implements com.codefollower.lealone.transaction.Transaction {

    private static final CommitHashMap commitHashMap = new CommitHashMap( //
            HBaseUtils.getConfiguration().getInt(TRANSACTION_COMMIT_CACHE_SIZE, DEFAULT_TRANSACTION_COMMIT_CACHE_SIZE), //
            HBaseUtils.getConfiguration().getInt(TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY,
                    DEFAULT_TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY));

    private static final TransactionStatusTable transactionStatusTable = TransactionStatusTable.getInstance();
    private static final ThreadPoolExecutor pool = CommandParallel.getThreadPoolExecutor();

    private final HBaseSession session;
    private final TimestampService timestampService;
    private final Transaction parent;
    private final Set<Transaction> children;
    private final Set<Transaction> nodeTransactions;

    //参与本次事务的其他SessionRemote
    private Map<String, SessionRemote> sessionRemoteCache;
    private CopyOnWriteArrayList<HBaseRow> undoRows;

    private long transactionId;
    private long commitTimestamp;
    private String hostAndPort;
    private boolean autoCommit;

    //只用于SessionRemotePool初始化SessionRemote
    public Transaction() {
        session = null;
        timestampService = null;
        parent = null;
        children = null;
        nodeTransactions = null;
    }

    public Transaction(HBaseSession session) {
        this(session, null);
    }

    public Transaction(HBaseSession session, Transaction parent) {
        this.session = session;
        this.parent = parent;
        timestampService = session.getTimestampService();
        children = New.hashSet();
        nodeTransactions = New.hashSet();

        sessionRemoteCache = New.hashMap();
        undoRows = new CopyOnWriteArrayList<HBaseRow>();

        //嵌套事务
        if (parent != null)
            autoCommit = false;
        else
            autoCommit = session.getAutoCommit();

        try {
            if (autoCommit)
                transactionId = timestampService.nextEven();
            else
                transactionId = timestampService.nextOdd();
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        hostAndPort = session.getHostAndPort();

        //初始化完transactionId和hostAndPort之后再加入parent，要用这两个变量来计算hashCode()
        if (parent != null)
            parent.addChild(this);
    }

    @Override
    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    @Override
    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public void setHostAndPort(String hostAndPort) {
        this.hostAndPort = hostAndPort;
    }

    @Override
    public String getHostAndPort() {
        return hostAndPort;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void addSessionRemote(String url, SessionRemote sessionRemote) {
        sessionRemoteCache.put(url, sessionRemote);
    }

    public SessionRemote getSessionRemote(String url) {
        return sessionRemoteCache.get(url);
    }

    public boolean isNested() {
        return parent != null;
    }

    public Transaction getParent() {
        return parent;
    }

    public void addChild(Transaction t) {
        children.add(t);
    }

    public void removeChild(Transaction t) {
        children.remove(t);
    }

    public Set<Transaction> getChildren() {
        return children;
    }

    public void addNodeTransaction(Transaction t) {
        nodeTransactions.add(t);
    }

    public Set<Transaction> getNodeTransactions() {
        return nodeTransactions;
    }

    public long getStartTimestamp() {
        return transactionId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hostAndPort == null) ? 0 : hostAndPort.hashCode());
        result = prime * result + (int) (transactionId ^ (transactionId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Transaction other = (Transaction) obj;
        if (hostAndPort == null) {
            if (other.hostAndPort != null)
                return false;
        } else if (!hostAndPort.equals(other.hostAndPort))
            return false;
        if (transactionId != other.transactionId)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "T-" + transactionId;
    }

    public void commit() {
        //if (!getAutoCommit() && (!undoRows.isEmpty() || isMaster())) { //从master发起的dml有可能没有undoRows
        if (!autoCommit) {
            try {
                if (session.isRoot()) {
                    if (sessionRemoteCache.size() > 0)
                        parallelCommit();
                    if (session.isMaster()) {
                        transactionStatusTable.addRecord(this, true);
                    } else {
                        long commitTimestamp = timestampService.nextOdd();
                        setCommitTimestamp(commitTimestamp);
                        checkConflict();
                        transactionStatusTable.addRecord(this, false);
                        ValidityChecker.cache.set(transactionId, commitTimestamp);
                    }
                } else {
                    long commitTimestamp = timestampService.nextOdd();
                    setCommitTimestamp(commitTimestamp);
                    checkConflict();
                    if (isNested())
                        transactionStatusTable.addRecord(this, false);
                    ValidityChecker.cache.set(transactionId, commitTimestamp);
                }

                for (Transaction t : children) {
                    t.commit();
                }
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
            if (transactionId < timestampService.first()) {
                //1. transactionId不可能小于region server启动时从TimestampServiceTable中获得的上一次的最大时间戳
                throw DbException.throwInternalError("transactionId(" + transactionId + ") < firstTimestampService("
                        + timestampService.first() + ")");
            } else if (!undoRows.isEmpty() && transactionId < commitHashMap.getLargestDeletedTimestamp()) {
                //2. Too old and not read only
                throw new RuntimeException("Too old startTimestamp: ST " + transactionId + " MAX "
                        + commitHashMap.getLargestDeletedTimestamp());
            } else {
                //3. write-write冲突检测
                for (HBaseRow row : undoRows) {
                    long oldCommitTimestamp = commitHashMap.getLatestWriteForRow(row.getHashCode());
                    if (oldCommitTimestamp != 0 && oldCommitTimestamp > transactionId) {
                        throw new RuntimeException("Write-write conflict: oldCommitTimestamp " + oldCommitTimestamp
                                + ", startTimestamp " + transactionId);
                    }
                }

                //不能把下面的代码放入第3步的for循环中，只有冲突检测完后才能put提交记录
                for (HBaseRow row : undoRows) {
                    commitHashMap.putLatestWriteForRow(row.getHashCode(), getCommitTimestamp());
                }
            }
        }
    }

    public void rollback() {
        if (!autoCommit) {
            try {
                if (session.isRoot() && sessionRemoteCache.size() > 0)
                    parallelRollback();

                undo();

                //避免java.util.ConcurrentModificationException
                if (!children.isEmpty()) {
                    Transaction[] childrenCopy = children.toArray(new Transaction[children.size()]);
                    for (Transaction t : childrenCopy)
                        t.rollback();
                }
            } catch (Exception e) {
                throw DbException.convert(e);
            } finally {
                //子事务(嵌套事务)rollback时从父事务中清除，但是commit时不需要从父事务中清除
                if (parent != null)
                    parent.removeChild(this);
                endTransaction();
            }
        }
    }

    private void parallelCommit() {
        parallel(true);
    }

    private void parallelRollback() {
        parallel(false);
    }

    private void parallel(final boolean commit) {
        int size = sessionRemoteCache.size();
        List<Future<Void>> futures = New.arrayList(size);
        for (final SessionRemote sessionRemote : sessionRemoteCache.values()) {
            futures.add(pool.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    if (sessionRemote.getTransaction() == null)
                        return null;
                    if (commit) {
                        sessionRemote.commitTransaction();
                        addNodeTransaction((Transaction) sessionRemote.getTransaction());
                    } else {
                        sessionRemote.rollbackTransaction();
                    }
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

    private void undo() {
        if (undoRows != null) {
            for (int i = undoRows.size() - 1; i >= 0; i--) {
                HBaseRow row = undoRows.get(i);
                row.getTable().removeRow(session, row, true);
            }
        }
    }

    private void endTransaction() {
        if (undoRows != null) {
            undoRows.clear();
            undoRows = null;
        }

        if (sessionRemoteCache != null) {
            for (SessionRemote sr : sessionRemoteCache.values()) {
                sr.setTransaction(null);
                SessionRemotePool.release(sr);
            }
            sessionRemoteCache = null;
        }
    }

    public void log(HBaseRow row) {
        if (!autoCommit) {
            undoRows.add(row);
        }
    }

    public boolean isUncommitted(long queryTimestamp) {
        Transaction parent = this.parent;
        if (parent == null)
            parent = this;
        else {
            while (parent.parent != null) {
                parent = parent.parent;
            }
        }
        return parent.isUncommittedRecursively(queryTimestamp);
    }

    public boolean isUncommittedRecursively(long queryTimestamp) {
        if (transactionId == queryTimestamp)
            return true;

        for (Transaction t : children) {
            if (t.isUncommittedRecursively(queryTimestamp))
                return true;
        }

        return false;
    }
}
