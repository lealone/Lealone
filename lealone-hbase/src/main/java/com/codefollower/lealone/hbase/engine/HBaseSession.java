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
package com.codefollower.lealone.hbase.engine;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import com.codefollower.lealone.command.Command;
import com.codefollower.lealone.command.Parser;
import com.codefollower.lealone.command.dml.Query;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.User;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.command.HBaseParser;
import com.codefollower.lealone.hbase.dbobject.HBaseSequence;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.result.HBaseSubqueryResult;
import com.codefollower.lealone.hbase.transaction.CommitHashMap;
import com.codefollower.lealone.hbase.transaction.Filter;
import com.codefollower.lealone.hbase.transaction.HBaseTransactionStatusTable;
import com.codefollower.lealone.hbase.transaction.RowKey;
import com.codefollower.lealone.hbase.transaction.Transaction;
import com.codefollower.lealone.hbase.transaction.TransactionException;
import com.codefollower.lealone.hbase.transaction.TransactionManager;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SubqueryResult;
import com.codefollower.lealone.util.New;

public class HBaseSession extends Session {
    private static final CommitHashMap commitHashMap = new CommitHashMap();
    private static final HBaseTransactionStatusTable transactionStatusTable = HBaseTransactionStatusTable.getInstance();

    /**
     * HBase的HMaster对象，master和regionServer不可能同时非null
     */
    private HMaster master;

    /**
     * HBase的HRegionServer对象，master和regionServer不可能同时非null
     */
    private HRegionServer regionServer;

    /**
     * 最初从Client端传递过来的配置参数
     */
    private Properties originalProperties;

    private Transaction transaction;
    private Transaction nontransaction;
    private Command currentCommand;

    private final List<HBaseRow> undoRows = New.arrayList();

    public HBaseSession(Database database, User user, int id) {
        super(database, user, id);
        try {
            nontransaction = TransactionManager.getNewTransaction();
        } catch (TransactionException e) {
            throw DbException.convert(e);
        }
    }

    public HMaster getMaster() {
        return master;
    }

    public void setMaster(HMaster master) {
        this.master = master;
    }

    public HRegionServer getRegionServer() {
        return regionServer;
    }

    public void setRegionServer(HRegionServer regionServer) {
        this.regionServer = regionServer;
    }

    public Properties getOriginalProperties() {
        return originalProperties;
    }

    public void setOriginalProperties(Properties originalProperties) {
        this.originalProperties = originalProperties;
    }

    @Override
    public HBaseDatabase getDatabase() {
        return (HBaseDatabase) database;
    }

    @Override
    public SubqueryResult createSubqueryResult(Query query, int maxrows) {
        return new HBaseSubqueryResult(this, query, maxrows);
    }

    @Override
    public Parser createParser() {
        return new HBaseParser(this);
    }

    @Override
    public HBaseSequence createSequence(Schema schema, int id, String name, boolean belongsToTable) {
        return new HBaseSequence(schema, id, name, belongsToTable);
    }

    @Override
    public void log(Table table, short operation, Row row) {
        // do nothing
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        super.setAutoCommit(autoCommit);
        //        if (!autoCommit && transaction == null) { //TODO 是否考虑支持嵌套事务
        //            try {
        //                transaction = TransactionManager.getNewTransaction();
        //            } catch (Exception e) {
        //                throw DbException.convert(e);
        //            }
        //        }
    }

    @Override
    public void begin() {
        super.begin();
        setAutoCommit(false);
    }

    @Override
    public void commit(boolean ddl) {
        if (!getAutoCommit() && transaction != null) {
            try {
                long tid = transaction.getTransactionId();
                long commitTimestamp = TransactionManager.getNewTimestamp();
                currentCommand.commitDistributedTransaction(tid, commitTimestamp);
                transactionStatusTable.addRecord(tid, commitTimestamp);
                Filter.committed.commit(tid, commitTimestamp);
            } catch (Exception e) {
                rollback();
                throw DbException.convert(e);
            } finally {
                endTransaction();
            }
        }

        super.commit(ddl);
    }

    @Override
    public void rollback() {
        if (!getAutoCommit() && transaction != null) {
            try {
                currentCommand.rollbackDistributedTransaction(transaction.getTransactionId());
            } catch (Exception e) {
                throw DbException.convert(e);
            } finally {
                endTransaction();
            }
        }

        super.rollback();
    }

    private void undo() {
        for (int i = undoRows.size() - 1; i >= 0; i--) {
            HBaseRow row = undoRows.get(i);
            row.getTable().removeRow(this, row);
        }
        undoRows.clear();
    }

    public Transaction getTransaction() {
        return transaction;
    }

    private void endTransaction() {
        transaction = null;
        undoRows.clear();
    }

    public Transaction beginTransaction(Command currentCommand) {
        if (getAutoCommit())
            transaction = nontransaction;
        if (transaction == null) {
            try {
                this.currentCommand = currentCommand;
                transaction = TransactionManager.getNewTransaction();
            } catch (TransactionException e) {
                throw DbException.convert(e);
            }
        }
        return transaction;
    }

    public void log(byte[] tableName, Row row) {
        if (!getAutoCommit() && transaction != null) {
            undoRows.add((HBaseRow) row);
            transaction.addRow(new RowKey(HBaseUtils.toBytes(row.getRowKey()), tableName));
        }
    }

    @Override
    public int commitDistributedTransaction(long transactionId, long commitTimestamp) {
        if (transaction != null && transaction.getTransactionId() == transactionId) {
            synchronized (HBaseSession.class) {
                long timestampOracle = Long.MIN_VALUE;
                if (transactionId < timestampOracle) { //TODO
                    throw new RuntimeException("Aborting transaction after restarting TSO");
                } else if (transaction.getRows().length > 0 && transactionId < commitHashMap.getLargestDeletedTimestamp()) {
                    // Too old and not read only
                    throw new RuntimeException("Too old startTimestamp: ST " + transactionId + " MAX "
                            + commitHashMap.getLargestDeletedTimestamp());
                } else {
                    // 1. check the write-write conflicts
                    for (RowKey r : transaction.getRows()) {
                        long oldCommitTimestamp = commitHashMap.getLatestWriteForRow(r.hashCode());
                        if (oldCommitTimestamp != 0 && oldCommitTimestamp > transactionId) {
                            throw new RuntimeException("Write-write conflict: oldCommitTimestamp " + oldCommitTimestamp
                                    + ", startTimestamp " + transactionId);
                        }
                    }

                    for (RowKey r : transaction.getRows()) {
                        commitHashMap.putLatestWriteForRow(r.hashCode(), commitTimestamp);
                    }
                }
            }
        }
        return 0;
    }

    @Override
    public int rollbackDistributedTransaction(long transactionId) {
        if (transaction != null && transaction.getTransactionId() == transactionId) {
            undo();
        }
        return 0;
    }
}
