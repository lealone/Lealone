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
package org.lealone.hbase.engine;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.lealone.command.Parser;
import org.lealone.command.router.FrontendSessionPool;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.User;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Database;
import org.lealone.engine.FrontendSession;
import org.lealone.engine.Session;
import org.lealone.hbase.command.CommandParallel;
import org.lealone.hbase.command.HBaseParser;
import org.lealone.hbase.command.dml.HBaseInsert;
import org.lealone.hbase.dbobject.HBaseSequence;
import org.lealone.hbase.result.HBaseRow;
import org.lealone.hbase.transaction.TimestampService;
import org.lealone.hbase.transaction.HBaseTransaction;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.util.New;

public class HBaseSession extends Session {
    private static final ThreadPoolExecutor pool = CommandParallel.getThreadPoolExecutor();

    /**
     * HBase的HMaster对象，master和regionServer不可能同时非null
     */
    private HMaster master;

    /**
     * HBase的HRegionServer对象，master和regionServer不可能同时非null
     */
    private HRegionServer regionServer;

    private TimestampService timestampService;
    private volatile HBaseTransaction transaction;

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
            this.timestampService = ((org.lealone.hbase.engine.HBaseRegionServer) regionServer).getTimestampService();
    }

    public boolean isRegionServer() {
        return regionServer != null;
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

    @Override
    public HBaseTransaction getTransaction() {
        if (transaction == null)
            beginTransaction();
        return transaction;
    }

    private void beginTransaction() {
        transaction = new HBaseTransaction(this);
    }

    private void endTransaction() {
        if (!frontendSessionCache.isEmpty()) {
            for (FrontendSession fs : frontendSessionCache.values()) {
                fs.setTransaction(null);
                FrontendSessionPool.release(fs);
            }

            frontendSessionCache.clear();
        }

        if (!isRoot())
            super.setAutoCommit(true);
    }

    @Override
    public void commit(boolean ddl) {
        commit(ddl, null);
    }

    @Override
    public void commit(boolean ddl, String allLocalTransactionNames) {
        if (this.transaction != null) {
            try {
                //避免重复commit
                HBaseTransaction transaction = this.transaction;
                this.transaction = null;
                if (allLocalTransactionNames == null)
                    allLocalTransactionNames = transaction.getAllLocalTransactionNames();
                List<Future<Void>> futures = null;
                if (!getAutoCommit() && frontendSessionCache.size() > 0)
                    futures = parallelCommitOrRollback(allLocalTransactionNames);

                transaction.commit(allLocalTransactionNames);
                super.commit(ddl);

                if (futures != null)
                    waitFutures(futures);
            } finally {
                endTransaction();
            }
        }
    }

    @Override
    public void rollback() {
        if (this.transaction != null) {
            try {
                HBaseTransaction transaction = this.transaction;
                this.transaction = null;
                List<Future<Void>> futures = null;
                if (!getAutoCommit() && frontendSessionCache.size() > 0)
                    futures = parallelCommitOrRollback(null);

                transaction.rollback();
                super.rollback();

                if (futures != null)
                    waitFutures(futures);
            } finally {
                endTransaction();
            }
        }
    }

    private List<Future<Void>> parallelCommitOrRollback(final String allLocalTransactionNames) {
        int size = frontendSessionCache.size();
        List<Future<Void>> futures = New.arrayList(size);
        for (final FrontendSession fs : frontendSessionCache.values()) {
            futures.add(pool.submit(new Callable<Void>() {
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

    public void log(HBaseRow row) {
        if (transaction == null)
            throw DbException.throwInternalError();
        transaction.log(row);
    }

    @Override
    public String getHostAndPort() {
        if (regionServer != null)
            return regionServer.getServerName().getHostAndPort();
        else
            return master.getServerName().getHostAndPort();
    }

    @Override
    public void addSavepoint(String name) {
        if (transaction != null) {
            if (!getAutoCommit() && frontendSessionCache.size() > 0)
                parallelSavepoint(true, name);

            transaction.addSavepoint(name);
        }
    }

    @Override
    public void rollbackToSavepoint(String name) {
        if (transaction != null) {
            if (!getAutoCommit() && frontendSessionCache.size() > 0)
                parallelSavepoint(false, name);

            transaction.rollbackToSavepoint(name);
        }
    }

    private void parallelSavepoint(final boolean add, final String name) {
        int size = frontendSessionCache.size();
        List<Future<Void>> futures = New.arrayList(size);
        for (final FrontendSession fs : frontendSessionCache.values()) {
            futures.add(pool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (add)
                        fs.addSavepoint(name);
                    else
                        fs.rollbackToSavepoint(name);
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
}
