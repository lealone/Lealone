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

import java.util.HashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.hbase.engine.HBaseConstants;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.metadata.TransactionStatusTable;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.value.Value;

public class Transaction implements com.codefollower.lealone.transaction.Transaction {

    private static final CommitHashMap commitHashMap = new CommitHashMap( //
            HBaseUtils.getConfiguration().getInt(TRANSACTION_COMMIT_CACHE_SIZE, DEFAULT_TRANSACTION_COMMIT_CACHE_SIZE), //
            HBaseUtils.getConfiguration().getInt(TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY,
                    DEFAULT_TRANSACTION_COMMIT_CACHE_ASSOCIATIVITY));

    private static final TransactionStatusTable transactionStatusTable = TransactionStatusTable.getInstance();

    private final HBaseSession session;
    private final TimestampService timestampService;

    private final String transactionName;
    private final long transactionId;
    private final boolean autoCommit;
    private long commitTimestamp;

    //协调者或参与者自身的本地事务名
    private StringBuilder localTransactionNamesBuilder;
    //如果本事务是协调者中的事务，那么在此字段中存放其他参与者的本地事务名
    private final ConcurrentSkipListSet<String> participantLocalTransactionNames = new ConcurrentSkipListSet<String>();

    private CopyOnWriteArrayList<HBaseRow> undoRows;
    private HashMap<String, Integer> savepoints;
    private final ConcurrentSkipListSet<Long> halfSuccessfulTransactions = new ConcurrentSkipListSet<Long>();

    private final byte[] transactionMetaAdd;
    private final byte[] transactionMetaDelete;

    public Transaction(HBaseSession session) {
        this.session = session;
        timestampService = session.getTimestampService();
        undoRows = new CopyOnWriteArrayList<HBaseRow>();

        autoCommit = session.getAutoCommit();

        transactionId = getNewTimestamp();
        String hostAndPort = session.getHostAndPort();
        transactionName = getTransactionName(hostAndPort, transactionId);

        transactionMetaAdd = Bytes.toBytes(hostAndPort + "," + transactionId + "," + HBaseConstants.Tag.ADD);
        transactionMetaDelete = Bytes.toBytes(hostAndPort + "," + transactionId + "," + HBaseConstants.Tag.DELETE);
    }

    public long getNewTimestamp() {
        if (autoCommit)
            return timestampService.nextEven();
        else
            return timestampService.nextOdd();
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

    //只从最顶层的事务提交
    public void commit(String allLocalTransactionNames) {
        if (!autoCommit && session.isRegionServer()) {
            try {
                //1. 获得提交时间戳
                commitTimestamp = timestampService.nextOdd();

                //2. 检测写写冲突
                checkConflict();

                //3. 更新事务状态表
                transactionStatusTable.addRecord(this, Bytes.toBytes(allLocalTransactionNames));

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
                        if (!transactionStatusTable.isFullSuccessful(hostAndPort, tid)) {
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
            for (HBaseRow row : undoRows) {
                commitHashMap.putLatestWriteForRow(row.hashCode(), getCommitTimestamp());
            }
        }
    }

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
    }

    public void log(HBaseRow row) {
        if (!autoCommit)
            undoRows.add(row);
    }

    public void addSavepoint(String name) {
        if (savepoints == null)
            savepoints = session.getDatabase().newStringMap();

        savepoints.put(name, undoRows.size());
    }

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
        HBaseRow row;
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

    public static String getTransactionName(String hostAndPort, long tid) {
        StringBuilder buff = new StringBuilder(hostAndPort);
        buff.append(':');
        buff.append(tid);
        return buff.toString();
    }

    public Put createHBasePut(byte[] defaultColumnFamilyName, Value rowKey) {
        Put put = new Put(HBaseUtils.toBytes(rowKey), getNewTimestamp());
        put.add(defaultColumnFamilyName, HBaseConstants.TRANSACTION_META, transactionMetaAdd);
        return put;
    }

    public Put createHBasePutWithDeleteTag(byte[] defaultColumnFamilyName, byte[] rowKey) {
        Put put = new Put(rowKey, getNewTimestamp());
        put.add(defaultColumnFamilyName, HBaseConstants.TRANSACTION_META, transactionMetaDelete);
        return put;
    }

    public void addHalfSuccessfulTransaction(Long tid) {
        halfSuccessfulTransactions.add(tid);
    }
}
