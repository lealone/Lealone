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

import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.Row;

public class GlobalTransaction extends TransactionBase {
    private static final CommitHashMap commitHashMap = new CommitHashMap(1000, 32);

    protected CopyOnWriteArrayList<Row> undoRows;

    public GlobalTransaction(Session session) {
        super(session);

        undoRows = new CopyOnWriteArrayList<>();
        transactionId = getNewTimestamp();
        transactionName = getTransactionName(TransactionManager.getHostAndPort(), transactionId);
    }

    public long getNewTimestamp() {
        if (autoCommit)
            return TimestampServiceTable.nextEven();
        else
            return TimestampServiceTable.nextOdd();
    }

    @Override
    public String toString() {
        return transactionName;
    }

    @Override
    protected void commitLocal(String allLocalTransactionNames) {
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

    @Override
    public long getSavepointId() {
        return undoRows.size();
    }

    @Override
    public void rollbackToSavepoint(long savepointId) {
        int size;
        Row row;
        while ((size = undoRows.size()) > savepointId) {
            row = undoRows.remove(size - 1);
            row.getTable().removeRow(session, row, true);
        }
    }

    @Override
    protected void endTransaction() {
        if (undoRows != null) {
            undoRows.clear();
            undoRows = null;
        }

        super.endTransaction();
    }

    public static String getTransactionName(String hostAndPort, long tid) {
        StringBuilder buff = new StringBuilder(hostAndPort);
        buff.append(':');
        buff.append(tid);
        return buff.toString();
    }
}
