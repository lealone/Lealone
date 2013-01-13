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
package com.codefollower.yourbase.table;

import java.util.List;

import org.apache.hadoop.hbase.Abortable;
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
import org.apache.zookeeper.data.Stat;

import com.codefollower.h2.engine.Database;
import com.codefollower.h2.engine.MetaRecord;
import com.codefollower.h2.result.SimpleRow;
import com.codefollower.h2.value.Value;
import com.codefollower.h2.value.ValueInt;
import com.codefollower.h2.value.ValueString;
import com.codefollower.yourbase.util.HBaseUtils;
import com.codefollower.yourbase.zookeeper.H2MetaTableTracker;

/**
 * 
 * 在HBase中存放一份H2数据库的meta table信息
 *
 */
public class H2MetaTable implements Abortable {

    private final static byte[] TABLE_NAME = Bytes.toBytes("H2_META_TABLE");
    private final static byte[] FAMILY = Bytes.toBytes("info");
    //private final static byte[] ID = Bytes.toBytes("id"); //ID作为rowKey
    private final static byte[] OBJECT_TYPE = Bytes.toBytes("type");
    private final static byte[] SQL = Bytes.toBytes("sql");

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public synchronized static void createTableIfNotExists() throws Exception {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);

        HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
        htd.addFamily(hcd);
        hcd.setMaxVersions(1);

        if (!admin.tableExists(TABLE_NAME)) {
            admin.createTable(htd);
        }
    }

    public synchronized static void dropTableIfExists() throws Exception {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        if (admin.tableExists(TABLE_NAME)) {
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        }
    }

    private final HTable table;
    private final ZooKeeperWatcher watcher;
    private final H2MetaTableTracker tracker;
    private final Database database;

    public H2MetaTable(Database database) throws Exception {
        createTableIfNotExists();
        this.database = database;
        table = new HTable(HBaseUtils.getConfiguration(), TABLE_NAME);
        watcher = new ZooKeeperWatcher(table.getConfiguration(), "H2MetaTableWatcher", this);
        tracker = new H2MetaTableTracker(watcher, this);
        tracker.start();
    }

    public H2MetaTableTracker getH2MetaTableTracker() {
        return tracker;
    }

    public Database getDatabase() {
        return database;
    }

    public void loadMetaRecords(List<MetaRecord> records) throws Exception {
        MetaRecord rec;
        for (Result r : table.getScanner(new Scan())) {
            if (r.isEmpty())
                continue;
            rec = getMetaRecord(r);
            records.add(rec);

            if (!tracker.contains(rec.getId()))
                ZKUtil.createNodeIfNotExistsAndWatch(watcher,
                        ZKUtil.joinZNode(H2MetaTableTracker.NODE_NAME, Integer.toString(rec.getId())), EMPTY_BYTE_ARRAY);
        }
    }

    public MetaRecord getMetaRecord(int id) {
        try {
            Result r = table.get(new Get(Bytes.toBytes(id)));
            if (!r.isEmpty())
                return getMetaRecord(r);
            else
                return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    public void addRecord(MetaRecord rec) {
        try {
            putRecord(rec, System.currentTimeMillis());
            String node = ZKUtil.joinZNode(H2MetaTableTracker.NODE_NAME, Integer.toString(rec.getId()));
            ZKUtil.createAndWatch(watcher, node, EMPTY_BYTE_ARRAY);
            tracker.updateIdVersion(rec.getId(), 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void updateRecord(MetaRecord rec) {
        try {
            long ts = System.currentTimeMillis();
            putRecord(rec, ts);
            String node = ZKUtil.joinZNode(H2MetaTableTracker.NODE_NAME, Integer.toString(rec.getId()));
            //setData会异步触发所有机器(包括本机)上的H2MetaTableTracker.nodeDataChanged
            //然后触发下列调用:
            //=>com.codefollower.h2.engine.Database.updateDatabaseObject(int)
            //  =>com.codefollower.h2.engine.Database.update(Session, DbObject)
            //      =>com.codefollower.h2.engine.Database.addMeta0(Session, DbObject, boolean)
            //          =>又到此方法
            //所以会造成循环
            synchronized (this) { //避免setData后立刻触发nodeDataChanged，此时IdVersion还未更新
                ZKUtil.setData(watcher, node, Bytes.toBytes(ts));
                //setData后watch不见了，所以要继续watch，监听其他人对此node的修改
                //ZKUtil.watchAndCheckExists(watcher, node);
                Stat stat = new Stat();
                ZKUtil.getDataAndWatch(watcher, node, stat);
                //这里记录下id的最新版本，触发nodeDataChanged时再检查一下是否版本一样，
                //如果不大于这里的版本那么就不再执行updateDatabaseObject操作
                tracker.updateIdVersion(rec.getId(), stat.getVersion());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void putRecord(MetaRecord rec, long ts) throws Exception {
        Put put = new Put(Bytes.toBytes(rec.getId()));
        put.add(FAMILY, OBJECT_TYPE, ts, Bytes.toBytes(rec.getObjectType()));
        put.add(FAMILY, SQL, ts, Bytes.toBytes(rec.getSQL()));
        //System.out.println("addRecord id: " + rec.getId() + ", sql=" + rec.getSQL());
        table.put(put);
    }

    public void removeRecord(int id) {
        try {
            //System.out.println("removeRecord id: " + id);
            //new Error().printStackTrace();
            Delete delete = new Delete(Bytes.toBytes(id));
            table.delete(delete);
            ZKUtil.deleteNodeFailSilent(watcher, ZKUtil.joinZNode(H2MetaTableTracker.NODE_NAME, Integer.toString(id)));
            tracker.removeId(id);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (table != null) {
            try {
                table.close();
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

    @Override
    public void abort(String why, Throwable e) {

    }

    @Override
    public boolean isAborted() {
        return false;
    }
}
