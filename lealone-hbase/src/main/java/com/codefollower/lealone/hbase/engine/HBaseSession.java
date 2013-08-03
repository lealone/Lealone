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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import com.codefollower.lealone.command.Parser;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.User;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.engine.SessionRemote;
import com.codefollower.lealone.hbase.command.CommandParallel;
import com.codefollower.lealone.hbase.command.HBaseParser;
import com.codefollower.lealone.hbase.command.dml.HBaseInsert;
import com.codefollower.lealone.hbase.dbobject.HBaseSequence;
import com.codefollower.lealone.hbase.metadata.TransactionStatusTable;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.transaction.CommitHashMap;
import com.codefollower.lealone.hbase.transaction.Filter;
import com.codefollower.lealone.hbase.transaction.RowKey;
import com.codefollower.lealone.hbase.transaction.TimestampService;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.transaction.Transaction;
import com.codefollower.lealone.util.New;

public class HBaseSession extends Session {
    private static final CommitHashMap commitHashMap = new CommitHashMap();
    private static final TransactionStatusTable transactionStatusTable = TransactionStatusTable.getInstance();
    private static final ThreadPoolExecutor pool = CommandParallel.getThreadPoolExecutor();

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

    private TimestampService timestampService;

    private Transaction transaction;

    private final List<HBaseRow> undoRows = New.arrayList();
    private final Set<RowKey> rowKeys = new HashSet<RowKey>();

    //参与本次事务的其他SessionRemote
    private Map<String, SessionRemote> sessionRemoteCache = New.hashMap();

    public void addSessionRemote(String url, SessionRemote sessionRemote) {
        sessionRemoteCache.put(url, sessionRemote);
    }

    public SessionRemote getSessionRemote(String url) {
        return sessionRemoteCache.get(url);
    }

    public HBaseSession(Database database, User user, int id) {
        super(database, user, id);
    }

    public HMaster getMaster() {
        return master;
    }

    public TimestampService getTimestampService() {
        return timestampService;
    }

    public void setMaster(HMaster master) {
        this.master = master;
        if (master != null)
            this.timestampService = HBaseMasterObserver.getTimestampService();
    }

    public boolean isMaster() {
        return master != null;
    }

    public HRegionServer getRegionServer() {
        return regionServer;
    }

    public void setRegionServer(HRegionServer regionServer) {
        this.regionServer = regionServer;
        if (regionServer != null)
            this.timestampService = ((com.codefollower.lealone.hbase.engine.HBaseRegionServer) regionServer)
                    .getTimestampService();
    }

    public boolean isRegionServer() {
        return regionServer != null;
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
    public Parser createParser() {
        return new HBaseParser(this);
    }

    @Override
    public HBaseInsert createInsert() {
        return new HBaseInsert(this);
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
        if (!autoCommit && transaction == null) {
            beginTransaction();
        }
    }

    @Override
    public void begin() {
        super.begin();
        setAutoCommit(false);
    }

    public Transaction getTransaction() {
        if (transaction == null)
            beginTransaction();
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public void commit(boolean ddl) {
        try {
            if (transaction != null) {
                if (!getAutoCommit() && (!undoRows.isEmpty() || isMaster())) { //从master发起的dml有可能没有undoRows
                    if (isRoot()) {
                        if (sessionRemoteCache.size() > 0)
                            parallelCommit();
                        if (isMaster()) {
                            transactionStatusTable.addRecord(transaction, isMaster());
                        } else {
                            long tid = transaction.getTransactionId();
                            long commitTimestamp = timestampService.nextOdd();
                            transaction.setCommitTimestamp(commitTimestamp);
                            transaction.setHostAndPort(getHostAndPort());
                            checkConflict();
                            transactionStatusTable.addRecord(transaction, isMaster());
                            Filter.committed.commit(tid, commitTimestamp);
                        }
                    } else {
                        long tid = transaction.getTransactionId();
                        long commitTimestamp = timestampService.nextOdd();
                        transaction.setCommitTimestamp(commitTimestamp);
                        checkConflict();
                        Filter.committed.commit(tid, commitTimestamp);
                    }
                }
            }
        } catch (Exception e) {
            rollback();
            throw DbException.convert(e);
        } finally {
            endTransaction();
        }

        super.commit(ddl);
    }

    @Override
    public void rollback() {
        try {
            if (transaction != null && !getAutoCommit()) {
                if (isRoot() && sessionRemoteCache.size() > 0) {
                    parallelRollback();
                }

                undo();
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            endTransaction();
        }
        super.rollback();
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
                        transaction.addChild(sessionRemote.getTransaction());
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
        for (int i = undoRows.size() - 1; i >= 0; i--) {
            HBaseRow row = undoRows.get(i);
            row.getTable().removeRow(this, row, true);
        }
        undoRows.clear();
    }

    private void beginTransaction() {
        if (transaction == null) {
            transaction = new Transaction();
            transaction.setAutoCommit(getAutoCommit());
            try {
                if (timestampService != null)
                    if (getAutoCommit())
                        transaction.setTransactionId(timestampService.nextEven());
                    else
                        transaction.setTransactionId(timestampService.nextOdd());
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }
    }

    public void endTransaction() {
        transaction = null;
        undoRows.clear();
        rowKeys.clear();

        for (SessionRemote sessionRemote : sessionRemoteCache.values()) {
            sessionRemote.setTransaction(null);
        }

        if (!isRoot())
            super.setAutoCommit(true);
    }

    public void log(byte[] tableName, Row row) {
        if (!getAutoCommit() && transaction != null) {
            undoRows.add((HBaseRow) row);
            rowKeys.add(new RowKey(HBaseUtils.toBytes(row.getRowKey()), tableName));
        }
    }

    private void checkConflict() {
        if (transaction != null) {
            long transactionId = transaction.getTransactionId();
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
                        commitHashMap.putLatestWriteForRow(r.hashCode(), transaction.getCommitTimestamp());
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        for (SessionRemote sr : sessionRemoteCache.values()) {
            SessionRemotePool.release(sr);
        }
        sessionRemoteCache = null;
        super.close();
    }

    @Override
    public String getHostAndPort() {
        if (regionServer != null)
            return regionServer.getServerName().getHostAndPort();
        else
            return master.getServerName().getHostAndPort();
    }
}
