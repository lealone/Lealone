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
package com.codefollower.yourbase.hbase.engine;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.dbobject.DbObject;
import com.codefollower.yourbase.dbobject.SchemaObject;
import com.codefollower.yourbase.dbobject.User;
import com.codefollower.yourbase.engine.ConnectionInfo;
import com.codefollower.yourbase.engine.Database;
import com.codefollower.yourbase.engine.MetaRecord;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.hbase.dbobject.table.MetaTable;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.util.New;

public class HBaseDatabase extends Database {

    private boolean isMaster;
    private boolean isRegionServer;
    private MetaTable metaTable;
    private HashMap<Integer, Pair> dbObjectMap;
    private boolean fromZookeeper;
    private boolean needToAddRedoRecord = true;

    public boolean isNeedToAddRedoRecord() {
        return needToAddRedoRecord;
    }

    public void setNeedToAddRedoRecord(boolean needToAddRedoRecord) {
        this.needToAddRedoRecord = needToAddRedoRecord;
    }

    private static class Pair {
        Session session;
        DbObject dbObject;
    }

    @Override
    public void init(ConnectionInfo ci, String cipher) {
        this.isMaster = "M".equalsIgnoreCase(ci.getProperty("SERVER_TYPE"));
        this.isRegionServer = "RS".equalsIgnoreCase(ci.getProperty("SERVER_TYPE"));
        dbObjectMap = New.hashMap();
        setCloseDelay(-1); //session关闭时不马上关闭数据库
        super.init(ci, cipher);
    }

    public boolean isMaster() {
        return isMaster;
    }

    public boolean isRegionServer() {
        return isRegionServer;
    }

    public boolean isFromZookeeper() {
        return fromZookeeper;
    }

    public void refreshMetaTable() {
        if (metaTable != null)
            metaTable.getMetaTableTracker().refresh();
    }

    @Override
    public synchronized void removeMeta(Session session, int id) {
        if (id > 0) {
            objectIds.clear(id);
            dbObjectMap.remove(id);
            if (!starting && isMaster && !fromZookeeper && metaTable != null)
                metaTable.removeRecord(id);
        }
    }

    @Override
    protected void openMetaTable(boolean create) {
        ArrayList<MetaRecord> records = New.arrayList();
        objectIds.set(0);
        starting = true;

        try {
            metaTable = new MetaTable(this);
            metaTable.loadMetaRecords(records);
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    public void executeMetaRecord(MetaRecord rec) {
        rec.execute(this, systemSession, eventListener);
    }

    public synchronized void removeDatabaseObject(int id) {
        if (isMaster())
            return;
        try {
            fromZookeeper = true;

            Pair p = dbObjectMap.get(id);
            //p为null时有可能是refresh时调用过了
            if (p != null && p.dbObject != null) {
                if (p.dbObject instanceof SchemaObject)
                    removeSchemaObject(p.session, (SchemaObject) p.dbObject);
                else
                    removeDatabaseObject(p.session, p.dbObject);

                isSysTableLockedThenUnlock(p.session);
            }
        } finally {
            fromZookeeper = false;
        }
    }

    @Override
    protected synchronized void addMeta(Session session, DbObject obj) {
        int id = obj.getId();
        if (id > 0 && !obj.isTemporary()) {
            objectIds.set(id);
            Pair p = new Pair();
            p.session = session;
            p.dbObject = obj;
            dbObjectMap.put(id, p);

            if (!starting && isMaster && !fromZookeeper && metaTable != null) {
                metaTable.addRecord(new MetaRecord(obj));
            }
        }
    }

    @Override
    public synchronized void update(Session session, DbObject obj) {
        lockMeta(session);
        int id = obj.getId();
        //removeMeta(session, id); //并不删除记录
        if (id > 0 && !starting) {
            dbObjectMap.remove(id);
            Pair p = new Pair();
            p.session = session;
            p.dbObject = obj;
            dbObjectMap.put(id, p);

            if (isMaster && !fromZookeeper && metaTable != null) {
                metaTable.updateRecord(new MetaRecord(obj));
            }
        }

    }

    @Override
    public synchronized void removeSchemaObject(Session session, SchemaObject obj) {
        //父SchemaObject删除时会顺带删除子SchemaObject，但是又会从ZK上接收到删除子SchemaObject的请求
        if (isRegionServer && dbObjectMap.get(obj.getId()) == null) {
            return;
        }
        super.removeSchemaObject(session, obj);
    }

    @Override
    protected synchronized void close(boolean fromShutdownHook) {
        super.close(fromShutdownHook);

        if (metaTable != null)
            metaTable.close();
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
