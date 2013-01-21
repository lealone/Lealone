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
package com.codefollower.yourbase.hbase.dbobject.table;

import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import com.codefollower.yourbase.engine.MetaRecord;
import com.codefollower.yourbase.hbase.engine.HBaseDatabase;
import com.codefollower.yourbase.hbase.util.HBaseUtils;
import com.codefollower.yourbase.hbase.zookeeper.MetaTableTracker;
import com.codefollower.yourbase.hbase.zookeeper.ZooKeeperAdmin;
import com.codefollower.yourbase.result.SimpleRow;
import com.codefollower.yourbase.value.Value;
import com.codefollower.yourbase.value.ValueInt;
import com.codefollower.yourbase.value.ValueString;

/**
 * 
 * 在HBase中存放数据库的元数据(比如create sql、alter sql等)
 * 
 * 此类不是线程安全的，只在Master端使用。
 *
 */
public class MetaTable {
    private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private final static int DEFAULT_MAX_REDO_RECORDS = 5000;

    private final static byte[] META_TABLE_NAME = Bytes.toBytes("YOURBASE_META_TABLE");
    private final static byte[] REDO_TABLE_NAME = Bytes.toBytes("YOURBASE_REDO_TABLE");
    private final static byte[] FAMILY = Bytes.toBytes("info");
    private final static byte[] ID = Bytes.toBytes("id");
    private final static byte[] OBJECT_TYPE = Bytes.toBytes("type");
    private final static byte[] SQL = Bytes.toBytes("sql");

    public synchronized static void createMetaTableIfNotExists() throws Exception {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
        hcd.setMaxVersions(1);

        HTableDescriptor htd = new HTableDescriptor(META_TABLE_NAME);
        htd.addFamily(hcd);
        if (!admin.tableExists(META_TABLE_NAME)) {
            admin.createTable(htd);
        }

        htd = new HTableDescriptor(REDO_TABLE_NAME);
        htd.addFamily(hcd);
        if (!admin.tableExists(REDO_TABLE_NAME)) {
            admin.createTable(htd);
        }
    }

    public synchronized static void dropMetaTableIfExists() throws Exception {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        if (admin.tableExists(META_TABLE_NAME)) {
            admin.disableTable(META_TABLE_NAME);
            admin.deleteTable(META_TABLE_NAME);
        }

        if (admin.tableExists(REDO_TABLE_NAME)) {
            admin.disableTable(REDO_TABLE_NAME);
            admin.deleteTable(REDO_TABLE_NAME);
        }
    }

    private final HTable metaTable;
    private final HTable redoTable;
    private final ZooKeeperWatcher watcher;
    private final MetaTableTracker tracker;
    private final HBaseDatabase database;
    private final int maxRedoRecords;
    private int nextRedoPos;

    public MetaTable(HBaseDatabase database) throws Exception {
        createMetaTableIfNotExists();
        this.database = database;
        maxRedoRecords = HBaseUtils.getConfiguration().getInt("yourbase.max.redo.records", DEFAULT_MAX_REDO_RECORDS);
        metaTable = new HTable(HBaseUtils.getConfiguration(), META_TABLE_NAME);
        redoTable = new HTable(HBaseUtils.getConfiguration(), REDO_TABLE_NAME);
        watcher = new ZooKeeperWatcher(HBaseUtils.getConfiguration(), "MetaTable", ZooKeeperAdmin.newAbortable());
        tracker = new MetaTableTracker(watcher, this);
        nextRedoPos = tracker.getRedoPos(false); //最初从1开始
        //master不需要监听元数据的变化，因为元数据的增删改只能从master发起
        if (!database.isMaster())
            tracker.start();
    }

    public MetaTableTracker getMetaTableTracker() {
        return tracker;
    }

    public void loadMetaRecords(List<MetaRecord> records) throws Exception {
        MetaRecord rec;
        for (Result r : metaTable.getScanner(new Scan())) {
            if (r.isEmpty())
                continue;
            rec = getMetaRecord(r);
            records.add(rec);
        }
    }

    public void addRecord(MetaRecord rec) {
        if (database.isMaster()) {
            try {
                addRedoRecord(rec, true);
                addMetaRecord(rec, System.currentTimeMillis());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void removeRecord(int id) {
        if (database.isMaster()) {
            try {
                MetaRecord rec = getMetaRecord(id);
                if (rec != null) {
                    addRedoRecord(rec, false);
                    metaTable.delete(new Delete(Bytes.toBytes(id)));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void redoRecords(int startPos, int stopPos) throws Exception {
        MetaRecord rec;
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startPos));
        scan.setStopRow(Bytes.toBytes(stopPos));
        for (Result r : redoTable.getScanner(scan)) {
            if (r.isEmpty())
                continue;
            rec = getRedoRecord(r);

            if (rec.getSQL() == null || rec.getSQL().length() == 0) {
                database.removeDatabaseObject(rec.getId());
            } else {
                database.executeMetaRecord(rec);
            }
        }
    }

    private void addRedoRecord(MetaRecord rec, boolean isAdd) throws Exception {
        if (nextRedoPos > maxRedoRecords)
            nextRedoPos = 1;

        Put put = new Put(Bytes.toBytes(nextRedoPos));
        put.add(FAMILY, ID, Bytes.toBytes(rec.getId()));
        put.add(FAMILY, OBJECT_TYPE, Bytes.toBytes(rec.getObjectType()));
        if (isAdd)
            put.add(FAMILY, SQL, Bytes.toBytes(rec.getSQL()));
        else
            put.add(FAMILY, SQL, EMPTY_BYTE_ARRAY);
        redoTable.put(put);

        nextRedoPos++;
        ZKUtil.setData(watcher, ZooKeeperAdmin.METATABLE_NODE, Bytes.toBytes(nextRedoPos));
    }

    private void addMetaRecord(MetaRecord rec, long ts) throws Exception {
        Put put = new Put(Bytes.toBytes(rec.getId()));
        put.add(FAMILY, OBJECT_TYPE, ts, Bytes.toBytes(rec.getObjectType()));
        put.add(FAMILY, SQL, ts, Bytes.toBytes(rec.getSQL()));
        //System.out.println("addRecord id: " + rec.getId() + ", sql=" + rec.getSQL());
        metaTable.put(put);
    }

    private MetaRecord getRedoRecord(Result r) {
        Value[] data = new Value[4];
        //id
        data[0] = ValueInt.get(Bytes.toInt(r.getValue(FAMILY, ID)));
        //head 未使用
        //data[1] = null;
        //type
        data[2] = ValueInt.get(Bytes.toInt(r.getValue(FAMILY, OBJECT_TYPE)));
        //sql
        data[3] = ValueString.get(Bytes.toString(r.getValue(FAMILY, SQL)));
        return new MetaRecord(new SimpleRow(data));

    }

    private MetaRecord getMetaRecord(Result r) {
        Value[] data = new Value[4];
        //id
        //data[0] = ValueInt.get(Bytes.toInt(r.getValue(FAMILY, ID)));
        data[0] = ValueInt.get(Bytes.toInt(r.getRow()));
        //head 未使用
        //data[1] = null;
        //type
        data[2] = ValueInt.get(Bytes.toInt(r.getValue(FAMILY, OBJECT_TYPE)));
        //sql
        data[3] = ValueString.get(Bytes.toString(r.getValue(FAMILY, SQL)));
        return new MetaRecord(new SimpleRow(data));

    }

    private MetaRecord getMetaRecord(int id) {
        try {
            Result r = metaTable.get(new Get(Bytes.toBytes(id)));
            if (!r.isEmpty())
                return getMetaRecord(r);
            else
                return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (metaTable != null) {
            try {
                metaTable.close();
            } catch (Exception e) {
                //ignore
            }
        }
        if (watcher != null) {
            try {
                watcher.close();
            } catch (Exception e) {
                //ignore
            }
        }
    }
}
