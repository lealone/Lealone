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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

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
import com.codefollower.lealone.hbase.metadata.TransactionStatusTable;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.result.HBaseSubqueryResult;
import com.codefollower.lealone.hbase.transaction.CommitHashMap;
import com.codefollower.lealone.hbase.transaction.Filter;
import com.codefollower.lealone.hbase.transaction.RowKey;
import com.codefollower.lealone.hbase.transaction.TimestampService;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SubqueryResult;
import com.codefollower.lealone.transaction.DistributedTransaction;
import com.codefollower.lealone.util.New;

public class HBaseSession extends Session {
    private static final CommitHashMap commitHashMap = new CommitHashMap();
    private static final TransactionStatusTable transactionStatusTable = TransactionStatusTable.getInstance();

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

    private Command currentCommand;
    private TimestampService timestampService;
    private boolean isFirst;

    private DistributedTransaction dt;

    private final List<HBaseRow> undoRows = New.arrayList();
    private final Set<RowKey> rowKeys = new HashSet<RowKey>();

    public HBaseSession(Database database, User user, int id) {
        super(database, user, id);
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
        if (regionServer != null) //不管Master的情况
            this.timestampService = ((com.codefollower.lealone.hbase.engine.HBaseRegionServer) regionServer)
                    .getTimestampService();
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
    public void commit(boolean ddl) {
        if (dt != null) {
            try {
                if (!getAutoCommit()) {
                    long tid = dt.getTransactionId();
                    long commitTimestamp = timestampService.nextOdd();
                    dt.setCommitTimestamp(commitTimestamp);
                    currentCommand.commitDistributedTransaction();
                    if (isFirst) {
                        transactionStatusTable.addRecord(dt);
                    }
                    Filter.committed.commit(tid, commitTimestamp);
                }
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
        if (dt != null && !getAutoCommit()) {
            try {
                currentCommand.rollbackDistributedTransaction();
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

    public DistributedTransaction getTransaction() {
        return dt;
    }

    private void endTransaction() {
        currentCommand = null;
        dt = null;
        undoRows.clear();
        rowKeys.clear();
    }

    public void log(byte[] tableName, Row row) {
        if (!getAutoCommit() && dt != null) {
            undoRows.add((HBaseRow) row);
            rowKeys.add(new RowKey(HBaseUtils.toBytes(row.getRowKey()), tableName));
        }
    }

    @Override
    public void commitDistributedTransaction(DistributedTransaction dt) {
        if (dt == this.dt) {
            long transactionId = dt.getTransactionId();
            synchronized (HBaseSession.class) {
                if (transactionId < timestampService.first()) {
                    //1. 事务开始时间不能小于region server启动时从TimestampServiceTable中获得的上一次的最大时间戳
                    throw new RuntimeException("Aborting transaction after restarting region server");
                } else if (!rowKeys.isEmpty() && transactionId < commitHashMap.getLargestDeletedTimestamp()) {
                    //2. Too old and not read only
                    throw new RuntimeException("Too old startTimestamp: ST " + transactionId + " MAX "
                            + commitHashMap.getLargestDeletedTimestamp());
                } else {
                    //3. write-write冲突检测
                    for (RowKey r : rowKeys) {
                        long oldCommitTimestamp = commitHashMap.getLatestWriteForRow(r.hashCode());
                        if (oldCommitTimestamp != 0 && oldCommitTimestamp > transactionId) {
                            throw new RuntimeException("Write-write conflict: oldCommitTimestamp " + oldCommitTimestamp
                                    + ", startTimestamp " + transactionId);
                        }
                    }

                    for (RowKey r : rowKeys) {
                        commitHashMap.putLatestWriteForRow(r.hashCode(), dt.getCommitTimestamp());
                    }
                }
            }
        }
    }

    @Override
    public void rollbackDistributedTransaction(DistributedTransaction dt) {
        if (dt == this.dt) {
            undo();
        }
    }

    public void beginTransaction(DistributedTransaction dt, Command currentCommand, boolean isDistributedTransaction,
            boolean isFirst) {
        if (this.dt == null) {
            try {
                dt.setAutoCommit(getAutoCommit());
                this.dt = dt;
                long transactionId;
                if (isDistributedTransaction || !getAutoCommit())
                    transactionId = timestampService.nextOdd();
                else
                    transactionId = timestampService.nextEven();

                this.dt.setTransactionId(transactionId);
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }

        this.currentCommand = currentCommand;
        this.isFirst = isFirst;
    }

    public DistributedTransaction getDistributedTransaction() {
        return dt;
    }

    public boolean isFirst() {
        return isFirst;
    }
}
