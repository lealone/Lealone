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
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.util.New;

public class Transaction implements com.codefollower.lealone.transaction.Transaction {

    private static final CommitHashMap commitHashMap = new CommitHashMap( //
            HBaseUtils.getConfiguration().getInt(TRANSACTION_COMMIT_CACHE_SIZE, DEFAULT_TRANSACTION_COMMIT_CACHE_SIZE), //
            HBaseUtils.getConfiguration().getInt(TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY,
                    DEFAULT_TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY));

    private final HBaseSession session;
    private final TimestampService timestampService;
    private final Transaction parent;

    private CopyOnWriteArrayList<Transaction> children;
    private CopyOnWriteArrayList<HBaseRow> undoRows;
    private CopyOnWriteArrayList<CommitInfo> commitInfoList;

    private long transactionId;
    private long commitTimestamp;
    private boolean autoCommit;

    public Transaction(HBaseSession session) {
        this(session, null);
    }

    public Transaction(HBaseSession session, Transaction parent) {
        this.session = session;
        this.parent = parent;
        timestampService = session.getTimestampService();
        children = new CopyOnWriteArrayList<Transaction>();
        undoRows = new CopyOnWriteArrayList<HBaseRow>();
        commitInfoList = new CopyOnWriteArrayList<CommitInfo>();

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

        //初始化完transactionId再加入parent，要用这个变量来计算hashCode()
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
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
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

    public long getStartTimestamp() {
        return transactionId;
    }

    @Override
    public String toString() {
        return "T-" + transactionId;
    }

    public void commit() {
        if (!autoCommit) {
            try {
                for (Transaction t : children)
                    t.commit();

                setCommitTimestamp(timestampService.nextOdd());
                checkConflict();
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
                for (Transaction t : children)
                    t.rollback();

                undo();
            } catch (Exception e) {
                throw DbException.convert(e);
            } finally {
                //子事务(嵌套事务)rollback时从父事务中清除，但是commit时不需要从父事务中清除
                if (parent != null)
                    parent.removeChild(this);
                endTransaction();
                releaseResources();
            }
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

        //children和commitInfoList两个字段在提交后还有用，如有必要可调用releaseResources()
    }

    public void log(HBaseRow row) {
        if (!autoCommit) {
            undoRows.add(row);
        }
    }

    public Transaction getRootTransaction() {
        Transaction parent = this.parent;
        if (parent == null)
            parent = this;
        else {
            while (parent.parent != null) {
                parent = parent.parent;
            }
        }

        return parent;
    }

    public boolean isUncommitted(long queryTimestamp) {
        return getRootTransaction().isUncommittedRecursively(queryTimestamp);
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

    @Override
    public void addCommitInfo(CommitInfo commitInfo) {
        commitInfoList.add(commitInfo);
    }

    @Override
    public CommitInfo[] getAllCommitInfo() {
        return getAllCommitInfo(true);
    }

    public CommitInfo[] getAllCommitInfo(boolean includeSelf) {
        ArrayList<CommitInfo> commitInfoList = New.arrayList(this.commitInfoList);

        if (includeSelf) {
            ArrayList<Transaction> list = New.arrayList();
            getTransactionRecursively(list);

            String hostAndPort = session.getHostAndPort();
            int len = list.size();
            long[] transactionIds = new long[len];
            long[] commitTimestamps = new long[len];
            for (int i = 0; i < len; i++) {
                transactionIds[i] = list.get(i).getTransactionId();
                commitTimestamps[i] = list.get(i).getCommitTimestamp();
            }

            commitInfoList.add(new CommitInfo(hostAndPort, transactionIds, commitTimestamps));
        }

        return commitInfoList.toArray(new CommitInfo[commitInfoList.size()]);
    }

    private void getTransactionRecursively(ArrayList<Transaction> list) {
        list.add(this);
        for (Transaction t : children)
            t.getTransactionRecursively(list);
    }

    @Override
    public void releaseResources() {
        for (Transaction t : children)
            t.releaseResources();

        if (children != null) {
            children.clear();
            children = null;
        }

        if (commitInfoList != null) {
            commitInfoList.clear();
            commitInfoList = null;
        }
    }
}
