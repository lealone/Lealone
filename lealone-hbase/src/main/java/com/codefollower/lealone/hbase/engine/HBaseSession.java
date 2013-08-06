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

import java.util.Properties;

import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import com.codefollower.lealone.command.Parser;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.User;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.engine.SessionRemote;
import com.codefollower.lealone.hbase.command.HBaseParser;
import com.codefollower.lealone.hbase.command.dml.HBaseInsert;
import com.codefollower.lealone.hbase.dbobject.HBaseSequence;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.transaction.TimestampService;
import com.codefollower.lealone.hbase.transaction.Transaction;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;

public class HBaseSession extends Session {
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
    private volatile Transaction transaction;

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

    @Override
    public Transaction getTransaction() {
        if (transaction == null)
            beginTransaction();
        return transaction;
    }

    private void beginTransaction() {
        transaction = new Transaction(this);
    }

    private void endTransaction() {
        if (transaction != null) {
            transaction = null;

            if (!isRoot())
                super.setAutoCommit(true);
        }
    }

    public synchronized void endNestedTransaction() {
        //上一级事务重新变成当前事务
        transaction = transaction.getParent();
    }

    public synchronized void beginNestedTransaction() {
        //新的嵌套事务变成当前事务
        transaction = new Transaction(this, transaction);
    }

    public void commitNestedTransaction() {
        if (transaction != null)
            transaction.commit();
    }

    public void rollbackNestedTransaction() {
        if (transaction != null)
            transaction.rollback();
    }

    @Override
    public void commit(boolean ddl) {
        if (transaction != null) {
            try {
                transaction.commit();
                super.commit(ddl);
            } finally {
                endTransaction();
            }
        }
    }

    @Override
    public void rollback() {
        if (transaction != null) {
            try {
                transaction.rollback();
                super.rollback();
            } finally {
                endTransaction();
            }
        }
    }

    public void log(HBaseRow row) {
        checkTransaction();
        transaction.log(row);
    }

    @Override
    public String getHostAndPort() {
        if (regionServer != null)
            return regionServer.getServerName().getHostAndPort();
        else
            return master.getServerName().getHostAndPort();
    }

    private void checkTransaction() {
        if (transaction == null)
            throw DbException.throwInternalError();
    }

    void addSessionRemote(String url, SessionRemote sessionRemote) {
        getTransaction();
        transaction.addSessionRemote(url, sessionRemote);
    }

    SessionRemote getSessionRemote(String url) {
        getTransaction();
        return transaction.getSessionRemote(url);
    }

}
