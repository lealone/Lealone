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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.DbObject;
import com.codefollower.lealone.dbobject.User;
import com.codefollower.lealone.engine.ConnectionInfo;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.DatabaseEngine;
import com.codefollower.lealone.engine.MetaRecord;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTableEngine;
import com.codefollower.lealone.hbase.metadata.DDLRedoTable;
import com.codefollower.lealone.hbase.metadata.MetaDataTable;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.util.New;

public class HBaseDatabase extends Database {

    private boolean isMaster;
    private boolean isRegionServer;
    private MetaDataTable metaDataTable;
    private DDLRedoTable ddlRedoTable;
    private boolean fromZookeeper;

    public HBaseDatabase(DatabaseEngine dbEngine) {
        super(dbEngine, false);
    }

    @Override
    public String getTableEngineName() {
        return HBaseTableEngine.NAME;
    }

    @Override
    public void init(ConnectionInfo ci, String cipher) {
        this.isMaster = "M".equalsIgnoreCase(ci.getProperty("SERVER_TYPE"));
        this.isRegionServer = "RS".equalsIgnoreCase(ci.getProperty("SERVER_TYPE"));
        setCloseDelay(-1); //session关闭时不马上关闭数据库
        super.init(ci, cipher);
    }

    public boolean isFromZookeeper() {
        return fromZookeeper;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public boolean isRegionServer() {
        return isRegionServer;
    }

    public void refreshDDLRedoTable() {
        if (ddlRedoTable != null)
            ddlRedoTable.getDDLRedoTableTracker().refresh();
    }

    @Override
    public synchronized void removeMeta(Session session, int id) {
        if (id > 0) {
            objectIds.clear(id);
            if (!starting && isMaster && metaDataTable != null)
                metaDataTable.removeRecord(id);
        }
    }

    @Override
    protected void openMetaTable(boolean create) {
        ArrayList<MetaRecord> records = New.arrayList();
        objectIds.set(0);
        starting = true;

        try {
            ddlRedoTable = new DDLRedoTable(this);
            metaDataTable = new MetaDataTable();
            metaDataTable.loadMetaRecords(records);
        } catch (Exception e) {
            throw DbException.convert(e);
        }

        Collections.sort(records);
        for (MetaRecord rec : records) {
            objectIds.set(rec.getId());
            rec.execute(this, systemSession, eventListener);
        }

        recompileInvalidViews(systemSession);
        starting = false;
    }

    protected HBaseSession createSystemSession(User user, int id) {
        return new HBaseSession(this, user, id);
    }

    public void executeSQL(String sql) {
        try {
            fromZookeeper = true;
            Prepared p = systemSession.prepare(sql, true);
            p.setExecuteDirec(true);
            p.update();
        } finally {
            fromZookeeper = false;
        }
    }

    @Override
    protected synchronized void addMeta(Session session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && !obj.isTemporary()) {
            objectIds.set(id);

            if (!starting && isMaster && metaDataTable != null) {
                metaDataTable.addRecord(new MetaRecord(obj));
            }
        }
    }

    @Override
    public synchronized void update(Session session, DbObject obj) {
        lockMeta(session);
        int id = obj.getId();
        //removeMeta(session, id); //并不删除记录
        if (id > 0 && !starting) {
            if (isMaster && metaDataTable != null) {
                metaDataTable.updateRecord(new MetaRecord(obj));
            }
        }
    }

    public synchronized void addDDLRedoRecord(HBaseSession session, String sql) {
        if (!starting && isMaster && ddlRedoTable != null) {
            ddlRedoTable.addRecord(session, sql);
        }
    }

    @Override
    protected synchronized void close(boolean fromShutdownHook) {
        super.close(fromShutdownHook);

        if (metaDataTable != null)
            metaDataTable.close();

        if (ddlRedoTable != null)
            ddlRedoTable.close();
    }

    @Override
    public synchronized HBaseSession createSession(User user) {
        if (exclusiveSession != null) {
            throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
        }
        HBaseSession session = new HBaseSession(this, user, ++nextSessionId);
        userSessions.add(session);
        trace.info("connecting session #{0} to {1}", session.getId(), databaseName);
        if (delayedCloser != null) {
            delayedCloser.reset();
            delayedCloser = null;
        }
        return session;
    }

    @Override
    public Connection getLobConnection() {
        return null;
    }

    @Override
    public boolean isMultiThreaded() {
        return true; //HBase总是使用多线程
    }
}
